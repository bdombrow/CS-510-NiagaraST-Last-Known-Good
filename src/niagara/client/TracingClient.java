package niagara.client;

import java.io.*;
import java.util.*;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public class TracingClient extends SimpleClient {
    private FileWriter fw;
    private ResultReader sr;
    private TracingHandler th;

    public TracingClient(String host, int port) {
        super(host, port);
        sr = new ResultReader();
        try {
            fw = new FileWriter("auction.client");
        } catch (IOException ioe) {
            throw new RuntimeException("IOException " + ioe.getMessage());
        }
        th = new TracingHandler(fw);

    }

    public void notifyNew(int id) {
        String results =
            ((SimpleConnectionReader) cm.getConnectionReader()).getResults();
        if (results.length() > 0)
            sr.addResult(results);
    }

    /**
     * @see niagara.client.UIDriverIF#notifyFinalResult(int)
     */
    public void notifyFinalResult(int id) {
            sr.finished();
    }

    public void processQuery(String queryText) {
        super.processQuery(queryText);
        // Start parsing
        SAXParser parser;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setFeature(
                "http://xml.org/sax/features/string-interning",
                true);
            factory.setFeature(
                "http://xml.org/sax/features/validation",
                false);
            parser = factory.newSAXParser();
            parser.parse(new InputSource(sr), th);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Parser Exception: " + e.getMessage());
        }
    }
    
    public static void main(String args[]) {
        // XXX vpapad: Copied from SimpleClient
        boolean queryfile = false;
        String qfName = null;

        // get the arguments
        String host = "localhost"; // the defaults
        int port = ConnectionManager.SERVER_PORT;

        int i = 0;
        while (i < args.length) {
            int starti = i;
            if (args[i].equals("-h")) {
                host = args[i + 1];
                i += 2;
            } else if (args[i].equals("-p")) {
                port = Integer.parseInt(args[i + 1]);
                i += 2;
            } else if (args[i].equals("-t")) {
                timeout = Integer.parseInt(args[i + 1]);
                i += 2;
            } else if (args[i].equals("--help")) {
                usage();
            } else if (args[i].equals("-qf")) {
                queryfile = true;
                qfName = args[i + 1];
                i += 2;
            }
            if (starti == i) {
                usage();
            }
        }

        TracingClient tc = new TracingClient(host, port);

        String query;

        try {
            if (!queryfile) {
                query = "";
                String line = "";

                BufferedReader br =
                    new BufferedReader(new InputStreamReader(System.in));
                do {
                    query = query + line;
                    line = br.readLine();
                } while (line != null);
                tc.processQuery(query);

            } else {
                char cbuf[] = new char[MAX_QUERY_LEN];
                query = getQueryFromFile(qfName, cbuf);
                tc.processQuery(query);
            }
        } catch (IOException ioe) {
            throw new RuntimeException("IOException " + ioe.getMessage());
        }
    }

    class TracingHandler extends DefaultHandler {
        FileWriter fw;

        public TracingHandler(FileWriter fw) {
            this.fw = fw;
        }
        /**
          * @see org.xml.sax.ContentHandler#startElement(String, String, String, Attributes)
          */
        public void startElement(
            String namespaceURI,
            String localName,
            String qName,
            Attributes atts)
            throws SAXException {
            try {
                String id = atts.getValue("id");
                String ts = atts.getValue("ts");
                if (id == null || ts == null)
                    return;
                fw.write(id);
                fw.write(",");
                fw.write(String.valueOf(System.currentTimeMillis()));
                fw.write("\n");
            } catch (IOException ioe) {
                throw new RuntimeException("IOException " + ioe.getMessage());
            }
        }
        /**
         * @see org.xml.sax.ContentHandler#endDocument()
         */
        public void endDocument() throws SAXException {
            try {
            fw.close();
            } catch (IOException ioe) {}
            System.exit(0);
        }

    }

    class ResultReader extends Reader {
        StringBuffer sb;
        boolean finished;

        public ResultReader() {
            sb = new StringBuffer();
            sb.append("<?xml version=\"1.0\"?><niagara:results>");
            finished = false;
        }

        public synchronized void addResult(String result) {
            // XXX vpapad: Have to remove header... Ugh!
            if (result.indexOf("<?xml") != -1)
                return;
            sb.append(result);
        }

        public void finished() {
            addResult("</niagara:results>");
            finished = true;
        }

        public boolean isFinished() {
            return finished;
        }
        
        public void close() {}
        
        /**
         * @see java.io.Reader#read(char[], int, int)
         */
        public synchronized int read(char[] cbuf, int off, int len)
            throws IOException {
            try {
                while (sb.length() == 0) {
                    if (finished) return -1;
                    wait(100);
                }

                int toCopy = len;
                if (sb.length() < len)
                    toCopy = sb.length();
                sb.getChars(0, toCopy, cbuf, off);
                sb.delete(0, toCopy);
                return toCopy;
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                System.exit(-1);
                return -1;
            }
        }
    }
}
