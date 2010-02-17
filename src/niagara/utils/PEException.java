package niagara.utils;

/**
 * ProgrammingErrorException.java
 * Created: March 30, 2000 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> PEException </code> A Programming Error Exception - to be thrown when
 * there is a programming error detected. This is similar to an assert. To be
 * used when the code detects a situation it thinks should never happen. Ideal
 * action is to pop this exception up to the top of the operator/query thread
 * and shut down the query with an error message - system doesn't need to crash.
 * 
 * @see Exception
 */

@SuppressWarnings("serial")
public class PEException extends RuntimeException {

	/**
	 * constructor
	 * 
	 * @see Exception
	 */
	public PEException() {
		super("Programming Error Exception:");
	}

	/**
	 * constructor
	 * 
	 * @param msg
	 *            the exception message
	 * @see Exception
	 */
	public PEException(String msg) {
		super("Programming Error Exception:" + msg);

	}

} // PEException
