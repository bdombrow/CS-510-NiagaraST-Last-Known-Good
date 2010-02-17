package niagara.xmlql_parser;

/**
 * 
 * This class is used for 'book keeping' in the generation of a chain of scan
 * nodes in the logical plan generation
 * 
 * 
 */

public class pnode {
	private pattern pat; // the pattern
	private int parent; // pointer to the pattern representing the parent

	/**
	 * Constructor
	 * 
	 * @param pattern
	 * @param index
	 *            of the parent pattern
	 */

	public pnode(pattern _pat, int _parent) {
		pat = _pat;
		parent = _parent;
	}

	/**
	 * @return the pattern
	 */

	public pattern getPattern() {
		return pat;
	}

	/**
	 * @return the index of the parent
	 */

	public int getParent() {
		return parent;
	}
}
