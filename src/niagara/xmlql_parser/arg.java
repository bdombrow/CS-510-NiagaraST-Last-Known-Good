package niagara.xmlql_parser;

/**
 * This class is used to represent the arguments of a function in XML-QL. As we
 * donot support functions in XML-QL, this class is not used at this time.
 * 
 */

public class arg {

	String var; // variable name
	String type; // variable type

	/**
	 * Constructor
	 * 
	 * @param variable
	 *            name
	 * @param variable
	 *            type
	 */

	public arg(String var, String type) {
		this.var = var;
		this.type = type;
	}

	public void dump() {
		System.out.println(var + " " + type);
	}

};
