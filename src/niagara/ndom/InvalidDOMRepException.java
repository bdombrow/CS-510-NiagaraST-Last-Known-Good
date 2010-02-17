package niagara.ndom;

/**
 * InvalidDOMRepException.java
 * Created: August 2, 2001 
 *
 * @author Kristin Tufte 
 * @version
 */

/**
 * <code> InvalidDOMRepException </code> Some code requires a certain DOM
 * represention, this error can be thrown when the wrong representation is
 * detected
 * 
 * @see Exception
 */

@SuppressWarnings("serial")
public class InvalidDOMRepException extends Exception {

	/**
	 * constructor
	 * 
	 * @see Exception
	 */
	public InvalidDOMRepException() {
		super("Invalid DOM Representation used");
	}

	/**
	 * constructor
	 * 
	 * @param msg
	 *            the exception message
	 * @see Exception
	 */
	public InvalidDOMRepException(String msg) {
		super(msg);
	}

}
