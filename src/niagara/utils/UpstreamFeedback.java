/**
 * 
 */
package niagara.utils;

/**
 * UpstreamFeedback objects are intended to be sent in reverse stream flow
 * direction. They can carry backward punctuation or string messages.
 * 
 * @author Rafael J. Fernandez-Moctezuma (rfernand@cs.pdx.edu)
 * @version 0.1u
 * @see CtrlFlags
 * @status in development 
 * 
 */
public class UpstreamFeedback {

	// Types of upstream feedback. An INFO type is provided for compatibility
	// with other message types.
	
	public enum FeedbackType{
		INFO,ASSUMED,DESIRED,DEMANDED
	}

	int ctrlFlag;
	FeedbackType feedbackType;
	String messageString;
	Punctuation punctuation;

	/**
	 * Constructor (Carrying a string message)
	 * 
	 * @param messageString
	 *            a message string
	 * @param ctrlFlag
	 *            a flag type
	 * @see DBThread
	 * @see CtrlFlags
	 */
	public UpstreamFeedback(String messageString, int ctrlFlag) {
		this.messageString = messageString;
		this.ctrlFlag = ctrlFlag;
		this.feedbackType = FeedbackType.INFO;
	}

	/**
	 * Constructor (Carrying punctuation)
	 * 
	 * @param punctuation
	 *            a punctuation object
	 * @param feedbackType
	 *            type of feedback (ASSUMED, DESIRED, or DEMANDED)
	 */
	public UpstreamFeedback(FeedbackType f, Punctuation punctuation) {
		this.punctuation = punctuation;
		this.feedbackType = f;
	}

	/**
	 * 
	 * @return Type of upstream feedback
	 * @see UpstreamFeedback
	 */
	public FeedbackType getFeedbackType() {
		return feedbackType;
	}

	public Punctuation getPunctuation(){
		return punctuation;
	}
	
	/**
	 * getMessage should only work with upstream feedback of type "INFO".
	 * 
	 * @return Control message
	 */
	public String getControlMessage() {
		assert feedbackType == FeedbackType.INFO : "Unexpected use of Upstream Feedback object.";
		return messageString;
	}

	/**
	 * getControlFlag should only work with upstream feedback of type "INFO"
	 * 
	 * @return Control flag
	 */
	public int getControlFlag() {
		assert feedbackType == FeedbackType.INFO : "Unexpected use of Upstream Feedback object.";
		return ctrlFlag;
	}

}
