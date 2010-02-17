package niagara.connection_server;

@SuppressWarnings("serial")
public class InvalidPlanException extends Exception {
	public InvalidPlanException() {
		super("Invalid Plan Exception: ");
	}

	public InvalidPlanException(String msg) {
		super("Invalid Plan Exception: " + msg + " ");
	}
}