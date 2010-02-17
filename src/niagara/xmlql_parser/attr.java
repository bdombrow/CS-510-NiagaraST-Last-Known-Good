package niagara.xmlql_parser;

import niagara.logical.Variable;
import niagara.optimizer.colombia.Attrs;
import niagara.utils.CUtil;

/**
 * This class is used to store attribute name and its value.
 * 
 */

public class attr {

	private String name; // name of the attribute
	private data value; // its value

	/**
	 * Constructor
	 * 
	 * @param name
	 *            of the attribute
	 * @param value
	 *            of the attribute ( identifier, variable or schema attribute)
	 */

	public attr(String s, data d) {
		name = s;
		value = d;
	}

	/**
	 * @return the name of the attribute
	 */

	public String getName() {
		return name;
	}

	/**
	 * @return value of the attribute
	 */

	public data getValue() {
		return value;
	}

	/**
	 * if the value is a variable, then replace it with a schema attribute
	 * representing the position of the corresponding schema unit in the schema
	 * 
	 * @param variable
	 *            table that maps the variable to schema attribute
	 */

	public void replaceVar(varTbl vt) {
		int type = value.getType();
		schemaAttribute sa;

		if (type == dataType.VAR) {
			String var = (String) value.getValue();
			sa = vt.lookUp(var);
			// XXX vpapad: Ugh.. try to search for the variable
			// without the leading dollar sign
			if (sa == null && var.charAt(0) == '$') {
				sa = vt.lookUp(var.substring(1));
			}
			value = new data(dataType.ATTR, sa);
		}
	}

	/**
	 * prints this class on the standard output
	 * 
	 * @param number
	 *            of tabs before each line
	 */

	public void dump(int i) {
		CUtil.genTab(i);
		System.out.println("ATTR");
		CUtil.genTab(i);
		System.out.println(name);
		value.dump(i);
	}

	public void addRequiredVariables(Attrs al) {
		if (value.getType() == dataType.VAR) {
			String var = (String) value.getValue();
			if (var.charAt(0) == '$')
				var = var.substring(1);
			al.merge(new Attrs(new Variable(var)));
		}
	}
}
