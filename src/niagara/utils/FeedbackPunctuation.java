/**
 * 
 */
package niagara.utils;

import java.util.HashMap;

/**
 * @author rfernand
 * @version 1.0
 *
 */
public final class FeedbackPunctuation  {
	private FeedbackType _type;
	private HashMap<String, Double> _punctuation; 
	private Comparator _comparator;
	
	public static enum Comparator {LT, LE, E, GE, GT};
	
	public FeedbackType Type() {
		return _type;
	}
	
	public FeedbackPunctuation(FeedbackType type, HashMap<String, Double> punctuation, Comparator comparator) {
		this._type = type;
		this._punctuation = punctuation;
		this._comparator = comparator;
	}

	/**
	 * 
	 * Determines whether the elements sent are covered by the feedback punctuation.
	 * 
	 * Assumes we're comparing Doubles.
	 * 
	 * @param positions
	 * @param attributeNames
	 * @param tupleElements
	 * @return
	 */
	public Boolean match(int[] positions, String[] attributeNames, Object[] tupleElements) {

		Boolean result = true;
			switch(_comparator) {
			case LT:
				for(int pos:positions) {
					result = result && ( (Double)tupleElements[pos]  < _punctuation.get(attributeNames[pos]));
				}
				return result;
			case LE:
				for(int pos:positions) {
					result = result && ( (Double)tupleElements[pos]  <= _punctuation.get(attributeNames[pos]));
				}
				return result;
			case E:
				for(int pos:positions) {
					result = result && ( (Double)tupleElements[pos]  == _punctuation.get(attributeNames[pos]));
				}
				return result;
			case GE:
				for(int pos:positions) {
					result = result && ( (Double)tupleElements[pos]  >= _punctuation.get(attributeNames[pos]));
				}
				return result;
			case GT:
				for(int pos:positions) {
					result = result && ( (Double)tupleElements[pos]  > _punctuation.get(attributeNames[pos]));
				}
				return result;
			default:
				return false;
		}
	}
	
	
	public String toString() {
		return "[Feedback Punctuation].[Type = '" + _type.Type() + "'].[" + this._punctuation.toString() + " " + this._comparator.toString() + "]";
	}

}

