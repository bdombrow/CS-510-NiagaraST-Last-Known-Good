/**
 * 
 */
package niagara.utils;

import java.util.ArrayList;
import java.util.Iterator;

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
	
	//amit: empty constructor
	public FeedbackPunctuation()
	{
		this._type = FeedbackType.ASSUMED;
		_variables = new ArrayList<String>();
		_comparators = new ArrayList<Comparator>();
		_values = new ArrayList<String>();
	}
	
	public void remove(int pos)
	{
		_variables.remove(pos);
		_comparators.remove(pos);
		_values.remove(pos);
	}
	
	public void setName(int pos,String newName)
	{
		_variables.set(pos, newName);
	}	
	
	public String getValue(int pos)
	{
		return _values.get(pos);
	}
	
	public void setValue(int position, String value)
	{
		_values.set(position, value);
	}
	
	public static enum Comparator {LT, LE, E, GE, GT};
	
	public FeedbackType Type() {
		return _type;
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> Variables()
	{
		ArrayList<String> _vars = (ArrayList<String>)_variables.clone();
		return _vars;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Comparator> Comparators()
	{
		ArrayList<Comparator> _comps = (ArrayList<Comparator>)_comparators.clone();
		return _comps;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<String> Values()
	{
		ArrayList<String> _vals = (ArrayList<String>)_values.clone();
		return _vals;
	}
	
	@SuppressWarnings("unchecked")
	public ArrayList<String> Variables(int pos)
	{
		ArrayList<String> _vars = new ArrayList<String>(_variables.subList(pos, pos+1));
		
		return _vars;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Comparator> Comparators(int pos)
	{
		ArrayList<Comparator> _comps = new ArrayList<Comparator>(_comparators.subList(pos, pos+1));
		return _comps;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<String> Values(int pos)
	{
		ArrayList<String> _vals = new ArrayList<String>(_values.subList(pos, pos+1));
		return _vals;
	}
	
//	public ArrayList getPos(String varname)
//	{
//		ArrayList pos = new ArrayList<Integer>();
//		String vars[] = varname.split(" ");
//				
//		Iterator<String> iter = _variables.iterator();
//		
//		int i=0;
//		
//		while(iter.hasNext())
//		{
//				String v = vars. iter.next().equals();
//				
//					pos.add(new Integer(i));
//				i++;
//		}
//		
//		return pos;
//	}
	
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
		public Boolean match(int[] positions, Object[] tupleElements) {

		Boolean match = true;
		int i = 0;
		for(int pos:positions) {
			match = match && compare(_variables.get(i), ((BaseAttr)tupleElements[pos]).toASCII());
			i++;
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
	
	public Boolean equals(FeedbackPunctuation other) {
		return this.toString().equals(other.toString());
	}

}



