package niagara.utils;

import org.w3c.dom.*;
import java.util.StringTokenizer;

/**
 * This is the <code>StreamPunctuationElement</code> class that is the unit
 * of transfer of punctuation across a stream. Like tuples, punctuations
 * are arrays of Nodes.
 *
 * @version 1.0
 *
 * @see Stream
 */

public final class Punctuation extends Tuple {

	/***
	 * 
	 * Inherited from <code>Tuple</code>
	 * 
	 * @param partial If this is true, the punctuation represents a partial result;
     *                else it represents a final result
	 */
	public Punctuation (boolean partial) {
		super(partial);
	}

	/***
	 * 
	 * Inherited from <code>Tuple</code>
	 * 
     * @param partial If this is true, the punctuation represents a partial result;
     *                else it represents a final result
     * @param capacity The initial capacity of the punctuation
	 */
	public Punctuation (boolean partial, int capacity) {
		super(partial, capacity);
	}

	public Punctuation(Element ele) {
		super(ele);
	}

	public boolean isPunctuation() {
		return true;
	}

	/**
	 * This function determines if the given punctuation is equal to this
	 * punctuation
	 *
	 * @return true if the two punctuations are equal
	 */

	public boolean equals(Punctuation punct) {
		//Only compare the 'document' nodes

		//return nodeEquals(this.getAttribute(0), punct.getAttribute(0));
		boolean eq = true;
		int pos = 0;
		for (Object attr:tuple) {
			if (attr instanceof Node)
				eq = eq && nodeEquals((Node)attr, (Node)punct.getAttribute(pos++));
			else if (attr instanceof BaseAttr)
				eq = eq && ((BaseAttr)attr).eq((BaseAttr) punct.getAttribute(pos++));
		}
		return eq;
	}

	private boolean nodeEquals(Node nd1, Node nd2) {
		if (nd1.getNodeName().equals(nd2.getNodeName()) == false)
			return false;

		if (nd1.getNodeType() != nd2.getNodeType())
			return false;

		String stValue = nd1.getNodeValue();
		if (stValue == null) {
			if (nd2.getNodeValue() != null)
				return false;
		} else {
			if (stValue.equals(nd2.getNodeValue()) == false)
				return false;
		}

		NodeList nl1 = nd1.getChildNodes();
		NodeList nl2 = nd2.getChildNodes();
		if (nl1.getLength() != nl2.getLength())
			return false;

		boolean fEquals = true;
		for (int i=0; i < nl1.getLength() && fEquals; i++) {
			fEquals = nodeEquals(nl1.item(i), nl2.item(i));
		}

		return fEquals;
	}

	/**
	 * This function clones a stream tuple element and returns the clone
	 *
	 * @return a clone of the stream tuple element
	 */

	public Object clone() {

		// Create a new stream punctuation element with the same partial
		// semantics
		Punctuation returnElement = 
			new Punctuation(this.partial, tupleSize);

		// Add all the attributes of the current tuple to the clone
		returnElement.appendTuple(this);

		// Return the clone
		//
		return returnElement;
	}

	public boolean match(Tuple ste) {
		assert false : "Unsupported function! - Jenny";
	return false;
	}

//	private boolean matchNode(Object ndPunctO, Object ndTupleO) {
//		if(!(ndPunctO instanceof Node) || !(ndTupleO instanceof Node)) {
//			throw new PEException("KT This code does not support object attrs");
//		}
//		Node ndPunct = (Node) ndPunctO;
//		Node ndTuple = (Node) ndTupleO;
//		String stPunct = ndPunct.getNodeValue();
//		boolean fMatch = true;
//
//		if (stPunct == null) {
//			//Need to compare children of this node
//			//for now, assume order matters between the punctuation
//			// and the tuple.
//			NodeList nlPunct = ndPunct.getChildNodes();
//			int cChild = nlPunct.getLength();
//
//			//Special case: if the punct has only a text node, check if
//			// it is a wildcard. If so, we can exit with 'true'
//			if (cChild == 1 &&
//					nlPunct.item(0).getNodeType() == Node.TEXT_NODE) {
//				String st = nlPunct.item(0).getNodeValue();
//				if (st.equals("*"))
//					return true;
//			}
//
//			NodeList nlTuple = ndTuple.getChildNodes();
//			if (cChild != nlTuple.getLength())
//				fMatch = false;
//
//			for (int iChild = 0; iChild < cChild && fMatch == true; iChild++) {
//				fMatch = matchNode(nlPunct.item(iChild), nlTuple.item(iChild));
//			}
//		} else {
//			fMatch = matchValue(stPunct, ndTuple.getNodeValue());
//		}
//
//		return fMatch;
//	}

	public static boolean matchValue(String stPunct, String stValue) {
		boolean fMatch = false;

		if (stPunct == null)
			return false;

		if (stPunct.equals("*"))
			//wildcard, everything matches that.
			fMatch = true;
		else if (stPunct.charAt(0) == '(' || stPunct.charAt(0) == '[') {
			//range of values. See if that value we have is in that
			// range.
			boolean fMinIncl = stPunct.charAt(0) == '[';
			boolean fMaxIncl = stPunct.charAt(stPunct.length()-1) == ']';
			fMatch = checkInRange(fMinIncl, fMaxIncl, stPunct, stValue);
		} else if (stPunct.charAt(0) == '{') {
			//list of items. See if the tuple value matches an item
			// in the list
			StringTokenizer stok =
				new StringTokenizer(stPunct, "{}, ");
			while(fMatch == false && stok.hasMoreElements()) {
				String stTok = stok.nextToken();
				fMatch = stTok.equals(stValue);
			}
		} else {
			//must be a constant. They should be equal
			fMatch = stPunct.equals(stValue);
		}

		return fMatch;
	}

	private static boolean checkInRange(boolean fMinIncl,
			boolean fMaxIncl, String stPunct, String stValue) {
		//Find the ',' to determine stMin
		int i=1; //Skip the leading '(' or '['
		for (; i<stPunct.length() && stPunct.charAt(i) != ','; i++) ;
		String stMin = stPunct.substring(1,i).trim();
		String stMax = stPunct.substring(i+1,stPunct.length()-2).trim();
		boolean fMatch = false;

		//See if we can convert the strings to doubles. If so,
		// do a numeric comparison. Otherwise, string comparison
		try {
			double dblMin = Double.NEGATIVE_INFINITY,
			dblMax = Double.POSITIVE_INFINITY;
			if (stMin.length() != 0)
				dblMin = Double.parseDouble(stMin);
			if (stMax.length() != 0)
				dblMax = Double.parseDouble(stMax);
			double dblVal = Double.parseDouble(stValue);

			//Since we're still here, match on a numeric comparison
			fMatch = (dblMin < dblVal && dblVal < dblMax) ||
			(fMinIncl && dblMin == dblVal) ||
			(fMaxIncl && dblMax == dblVal);
		} catch (NumberFormatException ex) {
			//OK, we have to do a string compare
			int nMinComp = stValue.compareTo(stMin);
			int nMaxComp = stValue.compareTo(stMax);
			fMatch = (nMinComp > 0 && nMaxComp < 0) ||
			(fMinIncl && nMinComp == 0) ||
			(fMaxIncl && nMaxComp == 0);
		}

		return fMatch;
	}

	/*** 
	 * Copy of this punctuation, with space reserved for up to `size` attributes 
	 * */
	public Punctuation copy(int size) {
		// XML-QL query plans will come in with size = 0
		if (tupleSize > size)
			size = tupleSize;
		// Create a new stream tuple element with the same partial semantics
		Punctuation returnElement =
			new Punctuation(partial, size);

		// Add all the attributes of the current tuple to the clone
		System.arraycopy(tuple, 0, returnElement.tuple, 0, tupleSize);
		returnElement.tupleSize = tupleSize;

		// Return the clone 
		return returnElement;
	}

	/***
	 *  Copy parts of this punctuation to a new tuple, with space reserved for up to
	 * <code>size</code> attributes 
	 * */
	public Punctuation copy(int size, int attributeMap[]) {
		assert size >= attributeMap.length : "Insufficient tuple capacity";
		// Create a new stream tuple element with the same partial semantics
		Punctuation returnElement =
			new Punctuation (partial, size);

		Object[] newTuple = returnElement.tuple;
		for (int to = 0; to < attributeMap.length; to++) {
			int from = attributeMap[to];
			if (from >= 0)
				newTuple[to] = tuple[from];
		}
		returnElement.tupleSize = attributeMap.length;

		return returnElement;
	}

}






