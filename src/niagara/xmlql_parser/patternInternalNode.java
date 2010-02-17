package niagara.xmlql_parser;

import java.util.Vector;

import niagara.utils.CUtil;

@SuppressWarnings("unchecked")
/**
 *
 * This class extends pattern and represents an internal node in the
 * n-ary tree to represent patterns of the WHERE part
 *
 *
 */
public class patternInternalNode extends pattern {

	private Vector patternList; // list of children (patterns)

	/**
	 * Constructor
	 * 
	 * @param regular
	 *            expression
	 * @param list
	 *            of attributes
	 * @param list
	 *            of children
	 * @param element_as
	 *            or content_as variable
	 */

	public patternInternalNode(regExp re, Vector al, Vector pl, data bd) {
		super(re, al, bd);
		patternList = pl;
	}

	/**
	 * @return list of children
	 */
	public Vector getPatternList() {
		return patternList;
	}

	/**
	 * print to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int i) {
		pattern child;
		super.dump(i);
		CUtil.genTab(i);
		System.out.println("sub-pattern");
		for (int j = 0; j < patternList.size(); j++) {
			child = (pattern) patternList.elementAt(j);
			child.dump(i + 1);
		}
	}
}
