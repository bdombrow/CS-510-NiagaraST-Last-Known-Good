package niagara.client;

import java.io.*;
import java.util.*;

public class SimpleClient implements UIDriverIF {
    protected ConnectionManager cm;
    static final int MAX_QUERY_LEN = 10000;
    static String queryFilePath =
        "/disk/hopper/projects/disc/niagara/query_files/";

    // For partial result requests
    static int timeout;
    static Timer timer;

    //To get times
    long m_start, m_stop;

    AbstractConnectionReader acr;

    public SimpleClient(String host, int port) {
        cm =
            new ConnectionManager(
                new SimpleConnectionReader(host, port, this, new DTDCache()));
    }

    public SimpleClient(String host) {
        this(host, ConnectionManager.SERVER_PORT);
    }

    public SimpleClient() {}

    public interface ResultsListener {
        void notifyNewResults(String results);
        void notifyError(String error);
        void notifyFinal();
    }

    Vector resultListeners = new Vector();

    public void addResultsListener(ResultsListener rl) {
        resultListeners.addElement(rl);
    }

    public void notifyNew(int id) {
        String results = ((SimpleConnectionReader) acr).getResults();
        if (resultListeners.size() == 0) {
            System.out.println(results);
        } else {
            for (int i = 0; i < resultListeners.size(); i++) {
                ((ResultsListener) resultListeners.get(i)).notifyNewResults(
                    results);
            }
        }
    }

    public void setConnectionReader(AbstractConnectionReader acr) {
        this.acr = acr;
    }
    
    public void queryDone() {
	// KT-VPAPAD - debug statements for client deadlock error
	//System.out.println("DLDEBUG Thread " + Thread.currentThread().getName() + " requesting mutex on SimpleClient (queryDone)");
	synchronized(this) {
	    //System.out.println("DLDEBUG Thread " + Thread.currentThread().getName() + " has mutex on SimpleClient (queryDone)");
	    m_stop = System.currentTimeMillis();
	    notify();
	    System.err.println("Total time: " + (m_stop - m_start) + "ms.");
	}
	//System.out.println("DLDEBUG Thread " + Thread.currentThread().getName() + " released mutex on SimpleClient (queryDone)");

    }
    
    public void notifyFinalResult(int id) {
        queryDone();
        if (resultListeners.size() > 0) {
            for (int i = 0; i < resultListeners.size(); i++) {
                ((ResultsListener) resultListeners.get(i)).notifyFinal();
            }
        }
    }

    public void errorMessage(int id, String err) {
        if (resultListeners.size() == 0) {
	    System.err.println();
            System.err.println("SimpleClient Error: " + err);
	    // Wake up main thread which is holding the mutex on this 
	    // object, must wake up otherwise we deadlock in queryDone
	    // main thread may be waiting to get the server id
	    cm.queryError(id);
	    queryDone(); // prints time used
	}
        else {
	    queryDone();
            for (int i = 0; i < resultListeners.size(); i++) {
                ((ResultsListener) resultListeners.get(i)).notifyError(err);
            }
        }
	System.exit(-1);
    }

    protected static void usage() {
        System.err.println(
            "Usage: SimpleClient [-h host] [-t timeout] [-p port] [-qf QueryFileName]\n"
            + "[-x repetitions] [-d delay] [-w wait] [-o outputFileName]\n"
            + "[-quiet] [-explain] [-ch client host] [-cp client port]");
        System.exit(-1);
    }

    protected static ArrayList queryFiles = new ArrayList();
    protected static ArrayList repetitions = new ArrayList();
    protected static ArrayList delays = new ArrayList();
    protected static ArrayList waits = new ArrayList();
    protected static boolean queryfile = false;
    protected static String host;
    protected static int port;
    protected static boolean explain;
    // XXX vpapad: ugly... SimpleClient can't do this yet
    protected static String outputFileName;
    // XXX vpapad: even uglier... these only apply to MQPClient
    protected static boolean quiet;
    protected static String clientHost = "localhost";
    protected static int clientPort = 3020;
    
    protected static void parseArguments(String args[]) {
        // get the arguments
        host = "localhost"; // the defaults
        port = ConnectionManager.SERVER_PORT;

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
                queryFiles.add(args[i + 1]);
                repetitions.add(new Integer(1));
                delays.add(new Integer(0));
                waits.add(new Integer(0));
                i += 2;
            } else if (args[i].equals("-x")) {
                repetitions.set(
                    repetitions.size() - 1,
                    new Integer(args[i + 1]));
                i += 2;
            } else if (args[i].equals("-d")) {
                delays.set(delays.size() - 1, new Integer(args[i + 1]));
                i += 2;
            } else if (args[i].equals("-w")) {
                waits.set(waits.size() - 1, new Integer(args[i + 1]));
                i += 2;
            } else if (args[i].equals("-o")) {
                outputFileName = args[i + 1];
                i += 2;
            } else if (args[i].equals("-quiet")) {
                quiet = true;
                i++;
            } else if (args[i].equals("-explain")) {
                explain= true;
                i++;
            } else if (args[i].equals("-ch")) {
                clientHost = args[i + 1];
                i += 2;
            } else if (args[i].equals("-cp")) {
                clientPort = Integer.parseInt(args[i + 1]);
                i += 2;
            } else {
                usage();
            }

            if (starti == i) {
                usage();
            }
        }
    }
    
    protected void processQueries() {
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
		// kt - don't think this needs to be synch
		// I removed it - hope this doesn't cause trouble!
		// if it is synchronized, causes deadlock on error
		//System.out.println("DLDEBUG: Thread " + Thread.currentThread().getName() + " requesting mutex on SimpleClient (processQueries)");
		synchronized(this) {
		    //System.out.println("DLDEBUG: Thread " + Thread.currentThread().getName() + " got mutex on SimpleClient (processQueries)");
		    try {
			processQuery(query);
		    } catch (ClientException e) {
			// do nothing - error is printed elsewhere
		    }
		    //System.out.println("DLDEBUG: Thread " + Thread.currentThread().getName() + " releasing mutex on SimpleClient (processQueries)");
		    wait();
		}
		System.exit(0);
            } else {
		int nQueries = queryFiles.size();
                for (int i = 0; i < nQueries; i++) {
                    char cbuf[] = new char[MAX_QUERY_LEN];
                    query = getQueryFromFile((String) queryFiles.get(i), cbuf);
                    int reps = ((Integer) repetitions.get(i)).intValue();
                    int delay = ((Integer) delays.get(i)).intValue();
                    int wait = ((Integer) waits.get(i)).intValue();
		    for (int j = 0; j < reps; j++) {
			if (j > 0 && delay > 0)
			    Thread.sleep(delay);
			// kt - don't think this needs to be synch
			// I removed it - hope this doesn't cause trouble!
			// if it is synchronized, causes deadlock on error
			synchronized (this) {
			    try {
				processQuery(query);
			    } catch (ClientException ce) {
				// do nothing - error reported elsewhere
			    }
			    wait();
			}
		    }
		    if (i < nQueries - 1 && wait > 0)
			Thread.sleep(wait);
                }
                System.exit(0);
            }
        } catch (IOException e) {
            System.err.println(
                "SimpleClient: IO error while reading query or query file");
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
	    System.err.println("Unexpected Thread Interruption: " + e.getMessage());
	}
    }
    
    public static void main(String args[]) {
        parseArguments(args);
        SimpleClient sc = new SimpleClient(host, port);
        sc.processQueries();
    }

    protected static String getQueryFromFile(String fileName, char cbuf[])
        throws FileNotFoundException, IOException {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            br = new BufferedReader(new FileReader(queryFilePath + fileName));
        }

        /* read the file into the buffer */
        int qLen = br.read(cbuf, 0, MAX_QUERY_LEN);
        if (qLen == MAX_QUERY_LEN) {
            System.out.println(
                "Query exceeds maximum length (" + MAX_QUERY_LEN + ")");
            return null;
        }
        return new String(cbuf, 0, qLen); /* copies cbuf */

    }

    public void processQuery(String queryText) throws ClientException {
        System.err.println("Executing query " + queryText);
        if (queryText.equalsIgnoreCase("gc")) {
            cm.runGarbageCollector();
            return;
        } else if (queryText.equalsIgnoreCase("shutdown")) {
            cm.shutdownServer();
            return;
        }

        m_start = System.currentTimeMillis();
        Query q = null;
        if (explain) 
            q = new ExplainQPQuery(queryText);
        else
            q = QueryFactory.makeQuery(queryText);
        final int id =
            cm.executeQuery(q, Integer.MAX_VALUE);
        if (timeout > 0) {
            // Create a new timer thread as a daemon thread
            timer = new Timer(true);
            // Request partial results after timeout milliseconds
            timer.schedule(new TimerTask() {
                public void run() {
                    cm.requestPartial(id);
                }
            }, timeout);
        }
    }

    private static String getQueryFileName(StreamTokenizer stdIn) {
        try {
            String fn = "";
            int tt = stdIn.nextToken();
            while (tt != StreamTokenizer.TT_EOL
                && tt != StreamTokenizer.TT_EOF) {
                if (tt == StreamTokenizer.TT_NUMBER) {
                    System.out.println("Invalid file name");
                    return null;
                }
                fn += stdIn.sval;
            }
            return fn;
        } catch (java.io.IOException e) {
            System.out.println(
                "Error getting query file name " + e.getMessage());
            return null;
        }

    }

}
