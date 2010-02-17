package niagara.xmlql_parser;

/**
 * 
 * This class is used to store value with its type (like IDEN, VAR, ATTR, etc.)
 * 
 */

public class data {

	int type; // type of data
	Object value; // value of the data (String for IDEN and VAR,

	// schemaAttribute for ATTR)

	/**
	 * Constructor
	 * 
	 * @param type
	 *            of the data value
	 * @param value
	 *            of the data
	 */

	public data(int type, Object value) {
		this.type = type;
		this.value = value;
	}

	/**
	 * @return type of the data value
	 */
	public int getType() {
		return type;
	}

	/**
	 * @return data value (String for IDEN and VAR, schemaAttribute for ATTR)
	 */
	public Object getValue() {
		return value;
	}

	/**
	 * @param value
	 *            of this data object
	 */
	public void setValue(Object o) {
		value = o;
	}

	/**
	 * prints this object to the standard output
	 */
	public void dump() {
		if (value instanceof String)
			System.out.println(type + (String) value);
		else if (type == dataType.ATTR)
			((schemaAttribute) value).dump();
	}

	/*
	 * This function is used to get a ASCII representation of the data.
	 * Currently only handle the very basic case. In the future, it needs to
	 * handle every case. --Jianjun
	 */

	public String toString() {
		if ((type == dataType.IDEN) || (type == dataType.STRING)) {
			return (String) value;
		} else {
			System.err.println("not supported yet--Trigger System");
			return null;
		}
	}

	/**
	 * prints to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */
	public void dump(int depth) {
		for (int i = 0; i < depth; i++)
			System.out.print("\t");
		if (type == dataType.IDEN)
			System.out.print("IDEN: ");
		else if (type == dataType.STRING)
			System.out.print("STRING: ");
		else if (type == dataType.NUMBER)
			System.out.print("NUMBER: ");
		else if (type == dataType.VAR)
			System.out.print("VAR: ");
		if (value instanceof String)
			System.out.println("'" + (String) value + "'");
		else if (type == dataType.ATTR)
			((schemaAttribute) value).dump(depth);
	}

}
