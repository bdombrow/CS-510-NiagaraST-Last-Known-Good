package niagara.client;

import java.io.*;
import java.util.*;

public class SimpleClient implements UIDriverIF {
    ConnectionManager cm; 
    static final int MAX_QUERY_LEN = 2048;
    static String queryFilePath = "/disk/hopper/projects/disc/niagara/query_files/";

    public SimpleClient(String host, int port) {
	cm = new ConnectionManager(host, port, this, true);
    }

    public void notifyNew(int id) {}

    public void notifyFinalResult(int id) {
	System.exit(0);
    }

    public void errorMessage(String err) {
	System.err.println("SimpleClient error:" + err);
    }

    public static void main(String args[]) {
	if (args.length > 3) {
	    System.err.println("Usage: SimpleClient [-qf QueryFile.name | -sh] <host> <port>)");
	    System.exit(-1);
	}
	
	int i = 0;

	boolean shell = false;
	boolean queryfile = false;
	String qfName = null;

	if(args.length > i) {
	    if(args[i].equals("-qf")) {
		i++;
		queryfile = true;
		qfName = args[i];
		i++;
	    }
	}

	if(args.length > i) {
	    if(args[i].equals("-sh")) {
		i++;
		shell = true;
	    }
	}

	String host;
	if (args.length > i) 
	    host = args[i];
	else 
	    host = "localhost";

	int port;
	if (args.length > i+1)
	    port = Integer.parseInt(args[i+1]);
	else
	    port = ConnectionManager.SERVER_PORT;
	
	
	SimpleClient sc = new SimpleClient(host, port);
	
	String query;

	try {
	    if(!shell && !queryfile) {
		query = "";
		String line = "";
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		do {
		    query = query + line;
		    line = br.readLine();
		} while (line != null);

		processQuery(query, sc);
		return;

	    } else if (!shell) {
		char cbuf[] = new char[MAX_QUERY_LEN];
		query = getQueryFromFile(qfName, cbuf);
		processQuery(query, sc);
		return;
	    } else {
		char cbuf[] = new char[MAX_QUERY_LEN];
		boolean stop = false;
		StreamTokenizer stdIn =
		    new StreamTokenizer(new 
			BufferedReader(new InputStreamReader(System.in)));
		stdIn.eolIsSignificant(true);
		int tt; /* token type */
		while(!stop) {
		    System.out.println("Query file: ");
		    tt = stdIn.nextToken();
		    if(tt == StreamTokenizer.TT_WORD) {
			if(stdIn.sval.equalsIgnoreCase("quit")) {
			    stop = true;
			} else {
			    stdIn.pushBack();
			    String fn = getQueryFileName(stdIn);
			    /* read a file name from std in */
			    query = getQueryFromFile(fn, cbuf);
			    processQuery(query, sc);
			}
		    } else if (tt == StreamTokenizer.TT_EOL) {
			/* ignore eols */
		    } else if (tt == StreamTokenizer.TT_NUMBER ||
			       tt == StreamTokenizer.TT_EOF) {
			System.out.println("Invalid input");
		    } else {
			System.out.println("Bad token type");
		    }
		}
		return;
	    }
	} catch (IOException e) {
	    System.err.println("SimpleClient: IO error while reading query or query file");
	    e.printStackTrace();
	    System.exit(-1);
	}

    }

    
    private static String getQueryFromFile(String fileName, char cbuf[]) 
    throws FileNotFoundException, IOException{
	BufferedReader br;
	try {
	    br = new BufferedReader(new FileReader(fileName));
	} catch (FileNotFoundException e) {
	    br = new BufferedReader(new FileReader(queryFilePath + fileName));
	}

	/* read the file into the buffer */
	int qLen = br.read(cbuf, 0, MAX_QUERY_LEN);
	if(qLen == MAX_QUERY_LEN) {
	    System.out.println("Query exceeds maximum length (2048)");
	    return null;
	}
	return new String(cbuf, 0, qLen); /* copies cbuf */


    }

    private static void processQuery(String queryText, SimpleClient sc) {
	System.out.println("Executing query " + queryText);
	int id = sc.cm.executeQuery(QueryFactory.makeQuery(queryText), 
				    Integer.MAX_VALUE);
        System.out.println("Query executed");
	return;
    }
    
    private static String getQueryFileName(StreamTokenizer stdIn) {
	try {
	    String fn = "";
	    int tt = stdIn.nextToken();
	    while(tt != StreamTokenizer.TT_EOL 
		  && tt != StreamTokenizer.TT_EOF) {
		if(tt == StreamTokenizer.TT_NUMBER) {
		    System.out.println("Invalid file name");
		    return null;
		}
		fn += stdIn.sval;
	    }
	    return fn;
	} catch (java.io.IOException e) {
	    System.out.println("Error getting query file name " + e.getMessage());
	    return null;
	}

    }
	
}




