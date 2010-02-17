/**
 * DMException.java
 * Created: Thu Apr 22 13:04:59 1999
 *
 * @version
 */

package niagara.data_manager;

/**
 * base class for all exceptions in data manager
 * 
 * @see Exception
 */
@SuppressWarnings("serial")
public class DMException extends Exception {

	/**
	 * constructor
	 * 
	 * @see Exception
	 */
	public DMException() {
		super("Data Manager Exception:");
	}

	/**
	 * constructor
	 * 
	 * @param msg
	 *            the exception message
	 * @see Exception
	 */
	public DMException(String msg) {
		super("Data Manager Exception:" + msg);

	}

} // DMException
