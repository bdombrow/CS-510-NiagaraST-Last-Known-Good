/**
 * 
 */
package niagara.utils;

import java.util.ArrayList;

/**
 * @author rfernand
 * @version 1.0
 *
 */
public final class FeedbackPunctuation  {
	private FeedbackType _type;
	private ArrayList<String> _variables;
	private ArrayList<Comparator> _comparators;
	private ArrayList<String> _values;
	

	public static enum Comparator {LT, LE, E, GE, GT};
	
	public FeedbackType Type() {
		return _type;
	}
	
	private String comparatorRepresentation(Comparator c){
		switch(c) {
		case LT:
			return "<";
		case LE:
			return "<=";
		case E:
			return "=";
		case GE:
			return ">=";
		case GT:
			return ">";
		default:
			return "";
		}
	}
	
	public FeedbackPunctuation(FeedbackType type, ArrayList<String> variables, ArrayList<Comparator> comparators, ArrayList<String> values) {
		this._type = type;
		this._variables = variables;
		this._comparators = comparators;
		this._values = values;
	}
	
	/***
	 * Compares a variable value with the object. The comparison is done as a Long number.
	 * @param value
	 * @param variable
	 * @return
	 */
	public Boolean compare(String variable, String other) {
		
		int index = _variables.indexOf(variable);
		Comparator c = _comparators.get(index);
		Long otherValue = Long.valueOf(other);
		Long thisValue = Long.valueOf(_values.get(index));
	
		switch(c) {
		case LT:
			return otherValue < thisValue;
		case LE:
			return otherValue <= thisValue;
		case E:
			return otherValue.equals(thisValue);
		case GE:
			return otherValue >= thisValue;
		case GT:
			return otherValue > thisValue;
		default:
			return false;
		}

	}


	/***
	 * Checks if the named subset of a tuple matches the feedback punctuation.
	 * @param positions Positions to check
	 * @param attributeNames Array of variable names
	 * @param tupleElements Array of Tuple elements
	 * @return
	 */
	public Boolean match(int[] positions, String[] attributeNames, Object[] tupleElements) {
		Boolean match = true;
		int i = 0;
		for(int pos:positions) {
			match = match && compare(attributeNames[i++], tupleElements[pos].toString());
		}
		return match;
	}
	
	
	public String toString() {
		
		String punctuationContents = "";
		for(int i = 0; i < _variables.size(); i++) {
			punctuationContents += _variables.get(i) +" "+ comparatorRepresentation(_comparators.get(i)) + " " + _values.get(i) + ", ";
		}
		punctuationContents = punctuationContents.substring(0, punctuationContents.length()-2);
		
		return "[Feedback Type = '" + _type.Type() + "'].[" + punctuationContents + "]";
	}

}

