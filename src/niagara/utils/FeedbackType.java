/**
 * 
 */
package niagara.utils;

/**
 * @author rfernand
 * @version 1.0
 *
 */
public enum FeedbackType {

	ASSUMED("Assumed Punctuation"), 
	DEMANDED("Demanded Punctuation"),             
	DESIRED("Desired Punctuation");            

	private final String _type;

	FeedbackType(String type) {
		this._type = type;
	}

	/***
	 * 
	 * @return type of feedback
	 */
	public String Type() {
		return _type;
	}
	
}
