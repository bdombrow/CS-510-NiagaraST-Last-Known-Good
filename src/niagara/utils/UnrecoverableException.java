package niagara.utils;

/**
 * UnrecoverablException.java
 * Created: March 30, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> UnrecoverableExceptin </code> A UnrecoverableException - to be thrown
 * when an error occurs which the server can not recover from. Should cause
 * server to crash.
 */

@SuppressWarnings("serial")
public class UnrecoverableException extends RuntimeException {

	/**
	 * constructor
	 * 
	 * @see Exception
	 */
	public UnrecoverableException() {
		super("Unrecoverable Exception:");
	}

	/**
	 * constructor
	 * 
	 * @param msg
	 *            the exception message
	 * @see Exception
	 */
	public UnrecoverableException(String msg) {
		super("UnrecoverableException:" + msg);

	}

}
