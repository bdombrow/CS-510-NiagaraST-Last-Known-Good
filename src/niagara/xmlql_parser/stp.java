package niagara.xmlql_parser;

import java.util.Vector;

@SuppressWarnings("unchecked")
/**
 * This class is used in the parser to pass information from one 
 * production rule to another.
 *
 */
public class stp {

	regExp regularExp; // regular expression
	Vector attrList; // list of attribute-value pair

	/**
	 * Constructor
	 * 
	 * @param regular
	 *            expression
	 * @param list
	 *            of attribute-value pair
	 */

	public stp(regExp regularExp, Vector attrList) {

		this.regularExp = regularExp;
		this.attrList = attrList;
	}

	/**
	 * @return regular expression
	 */

	public regExp getRegExp() {
		return regularExp;
	}

	/**
	 * @return list of attribute-value (attr) pair
	 */

	public Vector getAttrList() {
		return attrList;
	}

	/**
	 * prints to the standard output
	 */

	public void dump() {
		System.out.println("stp:");
		if (regularExp instanceof regExpOpNode)
			((regExpOpNode) regularExp).dump(0);
		else
			((regExpDataNode) regularExp).dump(0);
		for (int i = 0; i < attrList.size(); i++)
			((attr) attrList.elementAt(i)).dump(0);
	}
}
