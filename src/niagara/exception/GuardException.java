package niagara.exception;

/***
 * 
 * Custom exception for use in guard-related functions.
 * 
 * @author rfernand
 * @version 1.0
 */
public class GuardException extends Exception {

	private static final long serialVersionUID = 2899212594717023281L;

	/***
	 * 
	 * @param message
	 *            Description of the exception to be thrown.
	 */
	public GuardException(String message) {
		super("Exception thrown while using Guard-related code. " + message);
	}

}
