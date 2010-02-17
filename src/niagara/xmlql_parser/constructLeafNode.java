package niagara.xmlql_parser;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.utils.CUtil;
import niagara.utils.PEException;

/**
 * 
 * This class is used for representing leaf node in the construct tree
 * 
 */

public class constructLeafNode extends constructBaseNode {
	private data leafData;

	/**
	 * constructor
	 * 
	 * @param leaf
	 *            data
	 */
	public constructLeafNode(data d) {
		super();
		leafData = d;
	}

	/**
	 * get the data
	 * 
	 * @return leaf data
	 */
	public data getData() {
		return leafData;
	}

	/**
	 * if this leaf data is a variable then repalce it with the schema attribute
	 * representing that variable
	 * 
	 * @param variable
	 *            table that maps variable to schema attribute
	 */
	public void replaceVar(varTbl vt) {
		schemaAttribute attr;
		int type = leafData.getType();
		if (type == dataType.VAR) {
			String var = (String) leafData.getValue();
			attr = vt.lookUp(var);
			// XXX vpapad: super ugly - must get "$" out of variables
			if (attr == null && var.charAt(0) == '$')
				attr = vt.lookUp(var.substring(1));
			if (attr == null) {
				System.out.println(vt.getVars());
				throw new PEException("Could not look up variable " + var);
			}
			leafData = new data(dataType.ATTR, attr);
		}
	}

	/**
	 * print leaf data to standard output
	 * 
	 * @param number
	 *            of tabs in the beginning of each line
	 */
	public void dump(int depth) {
		CUtil.genTab(depth);
		System.out.println("constructLeaf");
		leafData.dump(depth);
	}

	/**
	 * @see niagara.xmlql_parser.syntax_tree.constructBaseNode#requiredInputAttrs(Attrs)
	 */
	public Attrs requiredInputAttributes(Attrs attrs) {
		int type = leafData.getType();
		if (type == dataType.VAR) {
			String varName = (String) leafData.getValue();
			// XXX vpapad: super ugly - must get "$" out of variables
			if (varName.charAt(0) == '$')
				varName = varName.substring(1);
			Attribute var = attrs.getAttr(varName);
			// This should have been caught at compile time
			if (var == null)
				throw new PEException("Unknown variable: " + varName);
			return new Attrs(var);
		} else
			return new Attrs();
	}
}
