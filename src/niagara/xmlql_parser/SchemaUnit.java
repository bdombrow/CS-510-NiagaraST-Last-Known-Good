package niagara.xmlql_parser;

/**
 * This class is used to construct Schema for describing tuples during the
 * generation of logical plan tree. Corresponds to attributes of a Schema in a
 * traditional relational database.
 * 
 */

public class SchemaUnit {
	private regExp regexp = null; // Describes Tags
	private int index = -1; // back pointer to enclosing element

	/**
	 * constructor
	 * 
	 * @param regular
	 *            expression and parent index
	 **/

	public SchemaUnit(regExp reg, int ind) {
		regexp = reg;
		index = ind;
	}

	/**
	 * constructor
	 * 
	 * @param another
	 *            schemaunit to make a copy of (uses same regular expression )
	 */

	public SchemaUnit(SchemaUnit su) {
		regexp = su.regexp;
		index = su.index;
	}

	/**
	 * get the regular expression
	 * 
	 * @return regexp
	 **/

	public regExp getRegExp() {
		return (regexp);
	}

	/**
	 * get the parent index
	 * 
	 * @return index
	 **/
	public int getIndex() {
		return (index);
	}

	/**
	 * same as getIndex()
	 * 
	 * @return index of the parent
	 */

	public int getBackPtr() {
		return index;
	}

	/**
	 * prints the schemaunit to the standard output
	 */

	public void dump() {
		System.out.println("SchemaUnit :");
		if (regexp == null)
			System.out.println("NULL");
		else
			regexp.dump(0);
		System.out.println("parent : " + index);
	}
}
