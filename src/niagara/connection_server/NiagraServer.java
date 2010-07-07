package niagara.connection_server;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import niagara.ndom.DOMFactory;
import niagara.ndom.saxdom.BufferManager;
import niagara.query_engine.QueryEngine;

import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.http.handler.ResourceHandler;
import org.mortbay.jetty.servlet.ServletHandler;

/**
 * The main Niagara Server which receives all the client requests It has an
 * instance of query engine and a SEClient for contacting the SE Server
 */
public class NiagraServer {

	// SAXDOM
	private static final int SAXDOM_DEFAULT_NUMBER_OF_PAGES = 1024;
	private static final int SAXDOM_DEFAULT_PAGE_SIZE = 1024;
	private static boolean useSAXDOM = true;
	private static int saxdom_pages = SAXDOM_DEFAULT_NUMBER_OF_PAGES;
	private static int saxdom_page_size = SAXDOM_DEFAULT_PAGE_SIZE;

	private static boolean acceptHTTP = false;

	public static boolean RJFM = false;

	// The port for client communication
	private static int client_port = 9020;

	// The port for server-to-server communication
	protected static int server_port = 8020;
	private HttpServer httpServer;

	private static boolean shuttingDown;

	// For executing QE queries
	QueryEngine qe;
	// for managing all the client connections
	ConnectionManager connectionManager;
	// Client for contacting the search engine
	// SEClient seClient;

	private static boolean startConsole = false;

	// Catalog
	private static String catalogFileName = "catalog.xml";
	private static Catalog catalog = null;

	public static boolean KT_PERFORMANCE = true;
	// Set this to false if you want to completely #ifdef away
	// the instrumentation code
	public static boolean ALLOW_INSTRUMENTATION = true;
	public static boolean RUNNING_NIPROF = false;
	public static boolean TIME_OPERATORS = false;

	public static boolean DEBUG = false;
	public static boolean DEBUG2 = false;

	// logging options
	public static boolean LOGGING = true;

	public NiagraServer() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});

		try {

			// Read the catalog
			catalog = new Catalog(catalogFileName);

			// Create the query engine
			qe = new QueryEngine(this, catalog
					.getIntConfigParam("query threads"), catalog
					.getIntConfigParam("operator threads"));

			// Create and start the connection manager
			connectionManager = new ConnectionManager(client_port, this);

			if (startConsole) {
				Console console = new Console(this, System.in);
				console.setDaemon(true);
				console.start();
			}
			if (useSAXDOM)
				BufferManager.createBufferManager(saxdom_pages,
						saxdom_page_size);

			if (acceptHTTP) {
				// Start HTTP server for interserver communication
				httpServer = new HttpServer();
				SocketListener listener = new SocketListener();
				listener.setPort(server_port);
				listener.setAcceptorThreads(10); // XXX vpapad

				httpServer.addListener(listener);

				// Niagara queries and other services
				HttpContext hc = httpServer.addContext(null, "/servlet/*");
				ServletHandler sh = new ServletHandler();
				sh.addServlet("/communication",
						"niagara.connection_server.CommunicationServlet");
				sh.addServlet("/httpclient",
						"niagara.connection_server.HTTPClientServlet");
				sh.addServlet("/rivulet/*",
						"niagara.connection_server.HTTPRivuletServlet");

				hc.addHandler(sh);

				// Serve static content
				HttpContext static_context = httpServer.addContext(null,
						"/static/*");
				static_context.setMimeMapping("xml", "text/xml");
				static_context.setResourceBase(catalog
						.getConfigParam("web directory"));
				static_context.addHandler(new ResourceHandler());

				try {
					httpServer.start();
				} catch (Exception e) {
					cerr("Could not start HTTP server: " + e.getMessage());
				}
				sh.getServletContext().setAttribute("server", this);
			}
		} catch (ConfigurationError ce) {
			System.err.println(ce.getMessage());
			System.exit(-1);
		}
	}

	public static void main(String[] args) {
		// Initialize the QueryEngine parameters
		init(args);
		if (TIME_OPERATORS | RUNNING_NIPROF) {
			System.loadLibrary("profni");
		}
		new NiagraServer();
	}

	public static void init(String[] args) {
		// Print usage/help info if -help flag is given
		if (args.length == 1 && args[0].equals("-help")) {
			usage();
			return;
		}

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-quiet")) {
				ResultTransmitter.QUIET = true;
			} else if (args[i].equals("-full-tuple")) {
				ResultTransmitter.OUTPUT_FULL_TUPLE = true;
			} else if (args[i].equals("-disable-buf-flush")) {
				System.out.println("Buffer flush false");
				ResultTransmitter.BUF_FLUSH = false;
			} else if (args[i].equals("-console")) {
				startConsole = true;
			} else if (args[i].equals("-port")) {
				if ((i + 1) >= args.length) {
					cerr("Please supply a parameter to -client-port");
					usage();
				} else {
					try {
						client_port = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException nfe) {
						cerr("Invalid argument to -client-port");
						usage();
					}
				}
				i++; // Cover for argument
			} else if (args[i].equals("-server-port")) {
				if ((i + 1) >= args.length) {
					cerr("Please supply a parameter to -server-port");
					usage();
				} else {
					try {
						server_port = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException nfe) {
						cerr("Invalid argument to -server-port");
						usage();
					}
				}
				i++; // Cover for argument
			} else if (args[i].equals("-catalog")) {
				if ((i + 1) >= args.length) {
					cerr("Please supply a parameter to -catalog");
					usage();
				} else {
					catalogFileName = args[i + 1];
				}
				i++; // Cover for argument
			} else if (args[i].equals("-dom")) {
				if ((i + 1) >= args.length) {
					cerr("Please supply a parameter to -dom");
					usage();
				} else {
					DOMFactory.setImpl(args[i + 1]);
				}
				i++; // Cover for argument
			} else if (args[i].equals("-no-saxdom")) {
				useSAXDOM = false;
			} else if (args[i].equals("-saxdom-pages")) {
				saxdom_pages = parseIntArgument(args, i);
				i++; // Cover for argument
			} else if (args[i].equals("-saxdom-page-size")) {
				saxdom_page_size = parseIntArgument(args, i);
				i++; // Cover for argument
			} else if (args[i].equals("-run-niprof")) {
				RUNNING_NIPROF = true;
			} else if (args[i].equals("-time-operators")) {
				TIME_OPERATORS = true;
			} else if (args[i].equals("-accept-http")) {
				acceptHTTP = true;
			} else if (args[i].equals("-RJFM-debug")) {
				RJFM = true;
			} else if (args[i].equals("-debug")) {
				DEBUG = true;
			} else if (args[i].equals("-debug2")) {
				DEBUG = true;
				DEBUG2 = true;
			} else {
				cerr("Unknown option: " + args[i]);
				usage();
			}
		}
	}

	private static int parseIntArgument(String args[], int pos) {
		if ((pos + 1) >= args.length) {
			cerr("Please supply a parameter to " + args[pos]);
			usage();
		} else {
			try {
				return Integer.parseInt(args[pos + 1]);
			} catch (NumberFormatException nfe) {
				cerr("Invalid argument to " + args[pos]);
				usage();
			}
		}

		System.exit(-1);
		return -1; /* Unreachable */
	}

	/**
	 * Print help and usage information
	 */
	private static void usage() {
		cout("");
		cout("Usage: java niagara.connection_server.NiagraServer [flags]");
		cout("\t-console      Rudimentary control of the server from stdin");
		cout("\t-client-port <number> Port number for client-server communication");
		cout("\t-server-port <number> Port number for inter-server communication");
		cout("\t-catalog <file> alternate catalog file (default is:"
				+ catalogFileName + ")");
		cout("\t-dom  <implementation name> Default DOM implementation.");
		cout("\t-saxdom  Use SAXDOM for input documents.");
		cout("\t-saxdom-pages <number> Number of SAXDOM pages.");
		cout("\t-saxdom-page-size <number> Size of each SAXDOM page.");
		cout("\t-accept-http Accept queries over HTTP.");
		cout("\t-RJFM-debug use for message passing debugging.");
		cout("\t-help   print this help screen");
		System.exit(-1);
	}

	private static void cout(String msg) {
		System.out.println(msg);
	}

	private static void cerr(String msg) {
		System.err.println(msg);
	}

	/**
	 * <code>getLocation</code>
	 * 
	 * @return the location <code>String</code> for this server
	 */
	public static String getLocation() {
		String ret = "";
		try {
			ret = InetAddress.getLocalHost().getHostAddress() + ":"
					+ server_port;
		} catch (java.net.UnknownHostException e) {
			cerr("Server host name is unknown! -- aborting...");
			System.exit(-1);
		}

		return ret;
	}

	public static boolean usingSAXDOM() {
		return useSAXDOM;
	}

	public static Catalog getCatalog() {
		return catalog;
	}

	public QueryEngine getQueryEngine() {
		return qe;
	}

	// Try to shutdown
	public void shutdown() {
		if (shuttingDown)
			return;
		shuttingDown = true;
		connectionManager.shutdown();
		qe.shutdown();
		catalog.shutdown();
		if (httpServer != null) {
			try {
				httpServer.stop(false);
				httpServer.destroy();
			} catch (InterruptedException e) {
				;
			}
		}
		info("Server has shut down.");
	}

	/**
	 * Simple method for info messages - just outputs to stdout now we can
	 * extend it later to do something fancier
	 */
	public static void info(String msg) {
		cout("INFO: " + msg);
	}

	public static void warning(String msg) {
		cerr("WARNING: " + msg);
	}
}
