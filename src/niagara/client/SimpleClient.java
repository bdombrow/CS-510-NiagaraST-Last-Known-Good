package niagara.client;

import java.io.*;

public class SimpleClient implements UIDriverIF {
    ConnectionManager cm; 

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
	if (args.length > 2) {
	    System.err.println("Usage: SimpleClient <host> <port>)");
	    System.exit(-1);
	}
	String host;
	if (args.length > 0) 
	    host = args[0];
	else 
	    host = "localhost";

	int port;
	if (args.length > 1)
	    port = Integer.parseInt(args[1]);
	else
	    port = ConnectionManager.SERVER_PORT;
	
	
	SimpleClient sc = new SimpleClient(host, port);
	

	String query = "", line = "";
	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	try {
	    do {
		query = query + line;
		line = br.readLine();
	    } while (line != null);
	}
	catch (Exception e) {
	    System.err.println("SimpleClient: an error occured while reading the query");
	    e.printStackTrace();
	    System.exit(-1);
	}
	
	sc.cm.executeQuery(QueryFactory.makeQuery(query), Integer.MAX_VALUE);
    }
}
