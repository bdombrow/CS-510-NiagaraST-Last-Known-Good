package niagara.exception;

/***
 * 
 * Custom exception for use in feedback-related functions.
 * 
 * @author rfernand
 * @version 1.0
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
