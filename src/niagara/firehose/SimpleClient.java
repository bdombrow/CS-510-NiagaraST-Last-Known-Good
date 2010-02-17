package niagara.firehose;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;

import niagara.utils.PEException;

class SimpleClient {
	public static void main(String args[]) {
		ClientMain cm = new ClientMain();
		if (cm.init(args))
			cm.run();
	}
}

// ClientMain is currently not reusable
class ClientMain {
	class Timer {
		private long start_tp;
		private long stop_tp;
		private boolean running;

		Timer() {
			running = false;
		}

		void start() {
			running = true;
			start_tp = System.currentTimeMillis();
		}

		void stop() {
			stop_tp = System.currentTimeMillis();
			running = false;
		}

		void print() {
			if (running == true) {
				System.err.println("Must stop timer before printing");
				return;
			}

			long total = stop_tp - start_tp;
			StringBuffer stTime = new StringBuffer("Seconds: ");
			stTime.append(String.valueOf(total / 1000));
			stTime.append(".");
			long msTotal = total % 1000;
			if (msTotal < 10)
				stTime.append("0");
			if (msTotal < 100)
				stTime.append("0");
			stTime.append(String.valueOf(msTotal));
			System.out.println(stTime.toString());
		}
	}

	// config file uses same attributes as in the queryplan
	// plus a few additional ones - quiet and client-wait
	private String host; // default to localhost
	private int port = FirehoseConstants.LISTEN_PORT;
	private int rate = 0; // as fast as possible
	private int datatype = FirehoseConstants.AUCTION_STREAM;
	private int num_gen_calls = 50;
	private boolean streaming = true;
	private String desc = "";
	private String desc2 = "";
	private int num_tl_elts = 1;
	private boolean prettyprint = true;
	private String trace = "";

	private boolean quiet = false;
	private boolean shutdown = false;

	private String fileString;

	protected boolean init(String args[]) {
		try {
			// set host name default
			host = InetAddress.getLocalHost().getHostName();

			if (args.length != 1)
				throw new InvalidUsageException(
						"Usage: SimpleClient initfile|shutdown");

			if (args[0].equalsIgnoreCase("shutdown")) {
				shutdown = true;
			} else {
				// read the configuration file
				readFile(args[0]);
				StringTokenizer tokenizer = new StringTokenizer(fileString,
						"\r\n\t:");
				while (tokenizer.hasMoreTokens()) {
					String attribute = tokenizer.nextToken();
					String value = tokenizer.nextToken();

					if (attribute.equalsIgnoreCase("host")) {
						host = value;
					} else if (attribute.equalsIgnoreCase("port")) {
						port = Integer.parseInt(value);
					} else if (attribute.equalsIgnoreCase("rate")) {
						rate = Integer.parseInt(value);
					} else if (attribute.equalsIgnoreCase("datatype")) {
						boolean found = false;
						for (int j = 0; j < FirehoseConstants.numDataTypes
								&& !found; j++) {
							if (value
									.equalsIgnoreCase(FirehoseConstants.typeNames[j])) {
								datatype = j;
								found = true;
							}
						}
						if (found == false) {
							throw new InvalidUsageException(
									"Invalid stream type " + value);
						}
					} else if (attribute.equalsIgnoreCase("num_gen_calls")) {
						num_gen_calls = Integer.parseInt(value);
					} else if (attribute.equalsIgnoreCase("streaming")) {
						if (value.equalsIgnoreCase("yes"))
							streaming = true;
						else
							streaming = false;
					} else if (attribute.equalsIgnoreCase("desc")) {
						desc = value;
					} else if (attribute.equalsIgnoreCase("desc2")) {
						desc2 = value;
					} else if (attribute.equalsIgnoreCase("num_tl_elts")) {
						num_tl_elts = Integer.parseInt(value);
					} else if (attribute.equalsIgnoreCase("prettyprint")) {
						if (value.equalsIgnoreCase("yes"))
							prettyprint = true;
						else
							prettyprint = false;
					} else if (attribute.equalsIgnoreCase("trace")) {
						trace = value;
					} else if (attribute.equalsIgnoreCase("quiet")) {
						if (value.equalsIgnoreCase("yes"))
							quiet = true;
						else
							quiet = false;
					} else if (attribute.equalsIgnoreCase("shutdown")) {
						if (value.equalsIgnoreCase("yes"))
							shutdown = true;
						else
							shutdown = false;
					} else {
						throw new InvalidUsageException(
								"Invalid Attribute Name: " + attribute);
					}
				}
			}

			if (shutdown) {
				shutdown_server();
				return false;
			}
		} catch (InvalidUsageException iue) {
			System.err.println(iue.getMessage());
			return false;
		} catch (UnknownHostException uhe) {
			System.err.println(uhe.getMessage());
			return false;
		} catch (FileNotFoundException fnfe) {
			System.err.println(fnfe.getMessage());
			return false;
		} catch (IOException ioe) {
			System.err.println(ioe.getMessage());
			return false;
		}

		return true;
	}

	protected void run() {
		try {
			Timer tm = new Timer();
			FirehoseClient fhclient = new FirehoseClient();

			tm.start();

			FirehoseSpec fhSpec = new FirehoseSpec(port, host, datatype, desc,
					desc2, num_gen_calls, num_tl_elts, rate, streaming,
					prettyprint, trace);

			InputStreamReader isr = new InputStreamReader(fhclient
					.open_stream(fhSpec));
			PrintWriter pw = new PrintWriter(System.out);
			char buffer[] = new char[1024];
			int numRead = 0;
			int count = 0;

			// KT - not sure if this is the right way to transfer from
			// is to output - use char reader?? or buffered reader??
			numRead = isr.read(buffer, 0, 1024);
			while (numRead > 0) {
				if (!quiet) {
					pw.write(buffer, 0, numRead);
				}
				count += numRead;
				numRead = isr.read(buffer, 0, 1024);
			}

			System.out.println();
			System.out.println("read " + count + " bytes");
			tm.stop();
			tm.print();
		} catch (IOException ioe) {
			System.err.println(ioe.getMessage());
		}
	}

	private int shutdown_server() {
		FirehoseClient fhclient = new FirehoseClient();
		FirehoseSpec fhSpec = new FirehoseSpec(port, host, datatype, desc,
				desc2, num_gen_calls, num_tl_elts, rate, streaming,
				prettyprint, trace);
		fhclient.shutdown_server(fhSpec);
		return 0;
	}

	// reads the configuration file into a string - file is expected to be short
	// so no problem, also no problem if slow
	private void readFile(String fileName) throws FileNotFoundException,
			IOException {
		char[] cbuf = new char[1024];
		FileReader reader = new FileReader(fileName);
		StringBuffer sbuf = new StringBuffer();
		int numRead = reader.read(cbuf, 0, 1024);
		sbuf.append(cbuf, 0, numRead);

		// make sure nothing is left in the file - these files should be really
		// short
		numRead = reader.read(cbuf, 0, 1024);
		if (numRead != -1)
			throw new PEException(
					"KT config file longer than expected, make quick fix here");

		fileString = sbuf.toString();
	}

	@SuppressWarnings("serial")
	class InvalidUsageException extends Exception {
		InvalidUsageException() {
			super("Invalid usage");
		}

		InvalidUsageException(String message) {
			super(message);
		}
	}
}
