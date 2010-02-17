package niagara.xmlql_parser;

import java.util.Vector;

@SuppressWarnings("unchecked")
/**
 * This class is used to represent the pattern of the InClause
 * e.g.
 *       <book>
 *          <author> $a </>
 *       </> ....
 *
 */
public class pattern {

	private regExp regularExp; // for the start tag
	private Vector attrList; // list of attributes (name, value) pair
	private data bindingData; // element_as or content_as

	/**
	 * Constructor
	 * 
	 * @param regular
	 *            expression for tag name
	 * @param list
	 *            of attributes
	 * @param element_as
	 *            or content_as variable
	 */

	public pattern(regExp re, Vector al, data bd) {
		regularExp = re;
		attrList = al;
		bindingData = bd;
	}

	/**
	 * @return regular expression for the tag name
	 */
	public regExp getRegExp() {
		return regularExp;
	}

	/**
	 * @return list of attributes
	 */
	public Vector getAttrList() {
		return attrList;
	}

	/**
	 * @return variable representing content_as or element_as
	 */
	public data getBindingData() {
		return bindingData;
	}

	/**
	 * print to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int i) {
		if (regularExp instanceof regExpOpNode)
			((regExpOpNode) regularExp).dump(i);
		else
			((regExpDataNode) regularExp).dump(i);
		for (int j = 0; j < attrList.size(); j++)
			((attr) attrList.elementAt(j)).dump(i);
		if (bindingData != null)
			bindingData.dump(i);
	}
}
