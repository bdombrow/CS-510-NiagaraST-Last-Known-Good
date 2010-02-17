package niagara.xmlql_parser;

import java.util.Vector;

@SuppressWarnings("unchecked")
/**
 *
 * This class is used to represent sets in XML-QL.
 * e.g $A IN {author, editor}
 *
 *
 */
public class set implements condition {
	private Vector set; // list of identifiers (author and editor in
	// the above example)
	private String var; // the set variable ($A in the above example)
	private regExp equivRE; // produced by ORing the identifiers

	// the above example will give :
	// |
	// / \
	// author editor

	/**
	 * Constructor
	 * 
	 * @param the
	 *            name of the variable
	 * @param the
	 *            list of identifiers
	 */

	public set(String _var, Vector _set) {
		set = _set;
		var = _var;
		equivRE = Util.getEquivRegExp(set);
	}

	/**
	 * @return the list of identifiers
	 */

	public Vector getSet() {
		return set;
	}

	/**
	 * @return the name of the variable
	 */

	public String getVar() {
		return var;
	}

	/**
	 * @return the equivalen regular expression
	 */

	public regExp getRegExp() {
		return equivRE;
	}

	/**
	 * print to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int i) {
		System.out.println("SET");
		System.out.println("variable: " + var);
		System.out.println("equivalent regular expression");
		equivRE.dump(1);
	}
}
