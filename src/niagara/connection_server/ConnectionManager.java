package niagara.connection_server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Date;

/**
 * The ConnectionManager listens on a well known port for connection requests.
 * When a connection is requested, the connection manager Creates a new
 * connection and a new socket. This new socket will receive all subsequent
 * messages sent over that connection.
 * 
 */
public class ConnectionManager implements Runnable {
	// The thread associated with the class
	private Thread thread;

	// the server that instantiated this connection manager
	private NiagraServer server;

	// doStop == true means Do not accept any more requests, and shutdown
	private boolean doStop;

	// The socket bound to a well know port that all clients
	// connect to the query engine on
	private ServerSocket queryEngineSocket;

	/**
	 *server is passed because it is used for getting access to dataManager and
	 * queryQueues
	 */
	public ConnectionManager(int queryEngineWellKnownPort, NiagraServer server) {
		// Init our ref to the NiagraServer
		this.server = server;

		// Create the main connection communication socket
		try {
			queryEngineSocket = new ServerSocket(queryEngineWellKnownPort);
		} catch (IOException e) {
			System.out.println("Failed to bind socket to port: "
					+ queryEngineWellKnownPort + "\n" + e);
			System.exit(1);
		}

		// Create a new java thread for running an instance of this object
		thread = new Thread(this, "Connection Manager");

		// Call the query thread run method
		thread.start();
	}

	/**
	 * This is the run method invoked by the Java thread - it simply waits on a
	 * socket for client connection messages
	 */
	public void run() {
		System.out.println("KT: Connection Manager up, listening on socket: "
				+ queryEngineSocket);

		try {
			// Calls to accept unblock every 500 msecs
			// to check for stop requests
			queryEngineSocket.setSoTimeout(500);
		} catch (SocketException e) {
			System.err.println("Could not set socket timeout!");
		}

		do {
			try {
				// Listen for the next client request
				Socket clientSocket = null;
				clientSocket = queryEngineSocket.accept();

				System.err.println("Query received: " + new Date()
						+ ", client socket = " + clientSocket);

				// Process the request
				// Hand over this socket to the Request handler
				// which will handle all the further requests
				new RequestHandler(clientSocket, server);
			} catch (SocketTimeoutException e) {
				if (doStop)
					return;
				else
					continue;
			} catch (IOException e) {
				System.out
						.println("Exception thrown while listening on QE server socket: "
								+ e);
				return;
			}
		} while (true);
	}

	/**
	 * Shut the connection manager down gracefully
	 */
	public void shutdown() {
		doStop = true;
	}
}
