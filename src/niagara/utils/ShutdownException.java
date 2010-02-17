package niagara.utils;

/**
 * ShutdownException is thrown when a SHUTDOWN message is received on a stream -
 * should propagate all the way up to PhysicalOperator. execute, where it is
 * handled
 * 
 * Shutdown should be used when query is shut down due to some execution
 * problem, user error, or client request.
 * 
 * @version 1.0
 * 
 */

@SuppressWarnings("serial")
public class ShutdownException extends Exception {

	public ShutdownException() {
		super("Query was shut down");
	}

	public ShutdownException(String msg) {
		super(msg);
	}
}
