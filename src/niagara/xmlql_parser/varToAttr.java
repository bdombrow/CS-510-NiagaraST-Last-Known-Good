package niagara.xmlql_parser;

import java.util.Vector;

/**
 * Stores all the schemaAttributes associated with a variable. This class is the
 * entered into the vector of the varTbl.
 * 
 */
@SuppressWarnings("unchecked")
public class varToAttr {
	private String var; // name of the variable
	private Vector attrs; // list of schemaAttribute associated with it

	/**
	 * Constructor
	 * 
	 * @param name
	 *            of the variable
	 * @param schemaAttribute
	 *            associated with it
	 */

	public varToAttr(String v, schemaAttribute a) {
		var = new String(v);
		attrs = new Vector();
		attrs.addElement(a);
	}

	/**
	 * Constructor
	 * 
	 * @param the
	 *            varToAttr to make a copy of
	 */

	public varToAttr(varToAttr v2a) {
		var = new String(v2a.var);
		attrs = new Vector();
		for (int i = 0; i < v2a.attrs.size(); i++)
			attrs.addElement(new schemaAttribute((schemaAttribute) v2a.attrs
					.elementAt(i)));
	}

	/**
	 * @return the list of attributes associated with this variable
	 */

	public Vector getAttributeList() {
		return attrs;
	}

	/**
	 * @return the name of the variable
	 */

	public String getVar() {
		return var;
	}

	/**
	 * @param another
	 *            schemaAttribute to be associated with this variable
	 */

	public void addAttribute(schemaAttribute a) {
		attrs.addElement(a);
	}

	/**
	 * @return the first schemaAttribute associated with this variable
	 */

	public schemaAttribute getAttribute() {
		return (schemaAttribute) attrs.elementAt(0);
	}

	/**
	 * print to the standard output
	 */

	public void dump() {
		System.out.println("var : " + var);
		for (int i = 0; i < attrs.size(); i++)
			((schemaAttribute) attrs.elementAt(i)).dump();
	}
}
