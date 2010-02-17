package niagara.client;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;

import niagara.utils.PEException;

import com.microstar.xml.XmlParser;

/**
 * This class establishes a connection with the server. Then calls the parse
 * method of the responseHandler to read the session document. Each session with
 * the server generates a separate document which conforms to response.dtd.
 * Inisde this document all the client queries are serviced
 * 
 */

class ConnectionReader extends AbstractConnectionReader implements Runnable {
	// member variables

	private XmlParser parser;

	/**
	 * This object handles callbacks for the session document.
	 */
	private ResponseHandler responseHandler;

	public ConnectionReader(String hostname, int port, UIDriverIF ui) {
		super(hostname, port, ui);
		initialize(ui);
	}

	/**
	 * The run method that invokes the parser
	 */
	public void run() {
		// Read the connection and throw the callbacks
		try {
			System.err.println("Parsing started");
			parser.parse(null, null, cReader);
			System.err.println("Parsing finished. Client Exiting.");
		} catch (EOFException e) {
			System.err.println("Server side ended the session");
			try {
				socket.close();
			} catch (IOException ee) {
				System.out.println("Error closing socket");
				ee.printStackTrace();
			}
			return;
		} catch (SocketException e) {
			System.err.println("Parser Closed down the socket");
			return;
		} catch (Exception e) {
			throw new PEException(
					"ConnectionReader: error parsing message from server - I think - KT");
		}

		System.exit(0);
	}

	void initialize(UIDriverIF ui) {
		// create a response handler and pass to it the registry
		responseHandler = new ResponseHandler(queryRegistry, ui);
		parser = new XmlParser();
		parser.setHandler(responseHandler);
	}
}
