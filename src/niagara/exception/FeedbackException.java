package niagara.exception;

/***
 * 
 * @author rfernand
 * Custom exception for use in feedback-related functions.
 */
public class FeedbackException extends Exception{

	/***
	 * 
	 * @param message Description of the exception to be thrown.
	 */
	public FeedbackException(String message){
		super("Exception thrown while using Feedback-related code. " + message);
}
	
	
}
