/**
 * $Id: Console.java,v 1.6 2002/12/10 00:56:21 vpapad Exp $
 */

package niagara.connection_server;

import java.io.*;

import niagara.ndom.saxdom.BufferManager;

/**
 * <code>Console</code> uses the standard input to provide
 * rudimentary control over the server.
 *
 */
public class Console extends Thread {
    
    NiagraServer server;
    BufferedReader br;

    public Console(NiagraServer server, InputStream in) {
        super("Console");
        this.server = server;
	br = new BufferedReader(new InputStreamReader(in));
    }

    public void run() {
	String command = "";
	while (true) {
	    try {
		command = br.readLine();
		}
	    catch (java.io.IOException e) {
		// ignored in previous code - KT
		System.err.println("IO exception reading from ??? " +
				   e.getMessage());
	    }
	    if (command.equals("exit")) {
		System.out.println("... server exiting.");
                server.shutdown();
		System.exit(0);
	    }
	    else if (command.equals("gc")) {
		System.gc();
		System.out.println("... garbage collection complete.");
	    }
            else if (command.equals("info")) {
                System.out.println("Free pages: " + BufferManager.numFreePages());
            }
	}
    }
}
