package niagara.connection_server;

import java.io.*;

/**
 * <code>Console</code> uses the standard input to provide
 * rudimentary control over the server.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 * @version 1.0
 */
public class Console extends Thread {
    
    NiagraServer server;
    BufferedReader br;

    public Console(NiagraServer server, InputStream in) {
        this.server = server;
	br = new BufferedReader(new InputStreamReader(in));
    }

    public void run() {
	String command = "";
	while (true) {
	    try {
		command = br.readLine();
	    }
	    catch (Exception e) {}
	    if (command.equals("exit")) {
		System.out.println("... server exiting.");
                server.shutdown();
		System.exit(0);
	    }
	    else if (command.equals("gc")) {
		System.gc();
		System.out.println("... garbage collection complete.");
	    }
	}
    }
}
