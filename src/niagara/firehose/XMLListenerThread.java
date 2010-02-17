package niagara.firehose;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

class XMLListenerThread extends Thread {

	private MsgQueue msg_queue;
	private ServerSocket listener_socket;
	private Socket client_socket;
	private XMLGenMessage message;
	private XMLFirehoseThread[] firehose_pool;
	private int pool_size;

	public XMLListenerThread(String str) {
		super(str);
	}

	public void run(int _port_num) {
		msg_queue = new MsgQueue();

		// create thread pool - uses msg_queue, must be done
		// after msg_queue is created
		create_firehose_pool();

		// create a socket to accept connections on
		try {
			listener_socket = new ServerSocket(_port_num);
		} catch (IOException e) {
			System.err.println("Listener socket creation error Port: "
					+ _port_num);
			shutdown_system();
			return;
		}

		System.err.println("Listening on port " + _port_num);
		while (true) {
			// get a message from the socket
			try {
				client_socket = listener_socket.accept();

				// parse
				// if a shutdown message is received, an exception will be
				// generated
				message = new XMLGenMessage();
				message.parse(new BufferedReader(new InputStreamReader(
						client_socket.getInputStream())));

				// put the message and reader in the queue for a XMLFirehose
				// thread
				// to pick up - message and socket are bundled into one queue
				// element
				// and delivered to the receiver together
				message.set_client_socket(client_socket);
				msg_queue.put(message);

			} catch (IOException e) {
				System.err.println("Listener socket accept failure");
				shutdown_system();
				return;
			} catch (ShutdownSystemException s) {
				System.err.println("Shutdown message received");
				shutdown_system();
				return;
			} catch (CorruptMsgException e) {
				System.err.println("Corrupt message: " + e.getMessage());
				// for some we send client an error message
			}
		}
	}

	private void create_firehose_pool() {
		pool_size = 5;
		firehose_pool = new XMLFirehoseThread[pool_size];
		String name;

		for (int i = 0; i < pool_size; i++) {
			name = "XML Generator Hose:" + i;
			firehose_pool[i] = new XMLFirehoseThread(name, msg_queue);
			firehose_pool[i].start();
		}
	}

	private void shutdown_system() {
		System.exit(0);
		return;
	}
}
