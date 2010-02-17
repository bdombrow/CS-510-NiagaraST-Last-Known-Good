package niagara.firehose;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import niagara.utils.PEException;

public class FirehoseClient {
	private static final int FCSTREAMSTATE_OPEN = 1;
	private static final int FCSTREAMSTATE_CLOSED = 2;

	// private static int fhId = -1;

	private int stream_state;
	private Socket sock;
	// private int id;
	private FirehoseSpec fhSpec;

	public FirehoseClient() {
		stream_state = FCSTREAMSTATE_CLOSED;
	}

	// add method to set spec fo FirehoseClient can be reused???
	// wait until we need it...KT

	// opens socket and sends open message
	// returns an id for this stream - to be used to close
	// the stream
	public InputStream open_stream(FirehoseSpec fhSpec) throws IOException {
		if (stream_state == FCSTREAMSTATE_OPEN) {
			throw new PEException("KT: attempting to open already open stream");
		}

		this.fhSpec = fhSpec;
		// id = getFhId();
		open_socket();
		send_open_message();
		stream_state = FCSTREAMSTATE_OPEN;
		return sock.getInputStream();
	}

	public void close_stream() throws IOException {
		// for now, just close the socket
		// when the sending thread detects socket closure,
		// it will just stop writing
		close_socket();
		stream_state = FCSTREAMSTATE_CLOSED;
	}

	public void shutdown_server(FirehoseSpec fhSpec) {
		try {
			if (stream_state == FCSTREAMSTATE_OPEN) {
				System.err
						.println("WARNING: shutting down server before closing connection");
				close_stream();
			}

			this.fhSpec = fhSpec;
			open_socket();
			String s = FirehoseConstants.SHUTDOWN + " ";

			sock.getOutputStream().write(s.getBytes());
			close_socket();
		} catch (IOException ioe) {
			// nothing to do...
			System.err.println("IOexception while shutting down server : "
					+ ioe.getMessage());
		}
	}

	public boolean is_open() {
		return (stream_state == FCSTREAMSTATE_OPEN);
	}

	private void send_open_message() throws IOException {
		StringBuffer s = new StringBuffer();
		s.append(FirehoseConstants.OPEN);
		s.append(" ");
		fhSpec.marshall(s);
		sock.getOutputStream().write(s.toString().getBytes());
	}

	private void open_socket() throws UnknownHostException, IOException {
		sock = new Socket(fhSpec.getListenerHostName(), fhSpec
				.getListenerPortNum());
	}

	private void close_socket() throws IOException {
		if (sock != null) {
			sock.close();
		}
	}

	protected void finalize() throws IOException {
		close_socket();
	}

	// private synchronized int getFhId() {
	// fhId++;
	// return fhId;
	// }

};
