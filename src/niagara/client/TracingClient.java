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
    public TracingClient(String host, int port, String outputFileName) {
        super(new TracingConnectionReader(host, port, new DTDCache(), outputFileName));
    }

    public void processQuery(String queryText) {
        System.err.println("Executing query " + queryText);

        m_start = System.currentTimeMillis();
        final int id =
            cm.executeQuery(
                new SynchronousQPQuery(queryText),
                Integer.MAX_VALUE);
    }

    public static void main(String args[]) {
        // XXX vpapad: Copied from SimpleClient
        boolean queryfile = false;
        String qfName = null;

        String outputFileName = null;
        
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
            } else if (args[i].equals("-o")) {
                outputFileName = args[i + 1];
                i += 2;
            }
            if (starti == i) {
                usage();
            }
        }

        TracingClient tc = new TracingClient(host, port, outputFileName);

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
    /**
     * @see niagara.client.UIDriverIF#notifyFinalResult(int)
     */
    public void notifyFinalResult(int id) {
    }

    /**
     * @see niagara.client.UIDriverIF#notifyNew(int)
     */
    public void notifyNew(int id) {
    }

}
