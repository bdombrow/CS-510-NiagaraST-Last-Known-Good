package niagara.xmlql_parser;

import java.util.Vector;

import niagara.logical.Variable;
import niagara.logical.predicates.Atom;
import niagara.logical.predicates.StringConstant;

/**
 * 
 * represents leaf of a pattern tree (iden/var/null)
 * 
 * 
 */

@SuppressWarnings("unchecked")
public class patternLeafNode extends pattern {

	private data expData; // leaf of the pattern

	/**
	 * Constructor
	 * 
	 * @param regular
	 *            expression for tags
	 * @param list
	 *            of attributes
	 * @param leaf
	 *            data (identifier or variable)
	 * @param element_as
	 *            or content_as var
	 */

	public patternLeafNode(regExp re, Vector al, data ed, data bd) {
		super(re, al, bd);
		expData = ed;
	}

	/**
	 * Constructor
	 * 
	 * @param regular
	 *            expression for tags
	 * @param list
	 *            of attributes
	 * @param element_as
	 *            or content_as var
	 */

	public patternLeafNode(regExp re, Vector al, data bd) {
		super(re, al, bd);
	}

	/** Constructor that uses the new predicate data structures */
	public patternLeafNode(regExp re, Vector al, Atom ed, data bd) {
		super(re, al, bd);
		if (ed.isVariable()) {
			expData = new data(dataType.VAR, ((Variable) ed).getName());
		} else {
			expData = new data(dataType.IDEN, ((StringConstant) ed).getValue());
		}
	}

	/**
	 * @return leaf data
	 */
	public data getExpData() {
		return expData;
	}

	/**
	 * displays on the satandard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */
	public void dump(int i) {
		super.dump(i);
		expData.dump(i);
	}
}
