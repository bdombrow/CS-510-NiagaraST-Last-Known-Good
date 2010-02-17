package niagara.xmlql_parser;

import java.util.Vector;

/**
 * This class is used to represent the skolem function in the start tag.
 * 
 */
@SuppressWarnings("unchecked")
public class skolem {
	private String name; // name of the skolem function
	private Vector varList; // list of variables or arguments

	/**
	 * Constructor
	 * 
	 * @param name
	 *            of the skolem function
	 * @param list
	 *            of arguments
	 */

	public skolem(String n, Vector vl) {
		name = n;
		varList = vl;
	}

	/**
	 * @return get the name of the function
	 */

	public String getName() {
		return name;
	}

	/**
	 * @return the list of variables/arguments
	 */

	public Vector getVarList() {
		return varList;
	}

	/**
	 * replace the variables with their corresponding schemaAttributes.
	 * 
	 * @param the
	 *            variable table that maps variable to their schemaAttribute
	 */

	public void replaceVar(varTbl vt) {
		schemaAttribute sa;
		for (int i = 0; i < varList.size(); i++) {
			sa = vt.lookUp((String) varList.elementAt(i));
			varList.setElementAt(sa, i);
		}
	}

	/**
	 * prints to the standard output
	 */

	public void dump() {
		System.out.println("skolem:");
		System.out.println(name);
		for (int i = 0; i < varList.size(); i++) {
			Object obj = varList.elementAt(i);
			if (obj instanceof String)
				System.out.println("\t" + (String) varList.elementAt(i));
			else
				((schemaAttribute) obj).dump(0);
		}
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof skolem))
			return false;
		skolem other = (skolem) obj;
		return name.equals(other.name) && varList.equals(other.varList);
	}
}
