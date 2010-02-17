package niagara.xmlql_parser;

import java.util.Vector;

/**
 * This class is used to describe the tuples generated during query processing.
 * It also helps in the generation of logical plan by capturing the parent-child
 * relationship among the elements scanned. The variables in the predicates are
 * replaced by attribute which has the position of the element in the Schema.
 * 
 * 
 */
@SuppressWarnings("unchecked")
public class Schema {
	private Vector tupleDes; // Vector of SchemaUnits

	// Constructor
	public Schema() {
		tupleDes = new Vector();
	}

	// constructor from an existing schema
	public Schema(Schema sc) {
		tupleDes = new Vector(sc.getVector());
	}

	/**
	 * This function gives the depth of a Nth schema unit
	 * 
	 * @param nth
	 *            element whose depth from the top element has to be calculated
	 * @return depth from the top element
	 * 
	 */

	public int level(int i) {
		int depth = 0;
		int j;
		if (i == 0)
			return -1;
		SchemaUnit currUnit = (SchemaUnit) tupleDes.elementAt(i);
		while ((j = currUnit.getBackPtr()) != 0) {
			depth++;
			currUnit = (SchemaUnit) tupleDes.elementAt(j);
		}
		return depth;
	}

	/**
	 * to add schemaunit at the end of the schema
	 * 
	 * @param schemaunit
	 *            to add
	 */

	public void addSchemaUnit(SchemaUnit su) {
		tupleDes.addElement(su);
	}

	/**
	 * to return the ith schemaunit
	 * 
	 * @param position
	 *            of the schemaunit to be returned
	 */

	public SchemaUnit getSchemaUnit(int i) {
		return (SchemaUnit) tupleDes.elementAt(i);
	}

	/**
	 * @return the number of schemaunits in this schema
	 */

	public int numAttr() {
		return tupleDes.size();
	}

	/**
	 * @return the vector representing the schema
	 */

	public Vector getVector() {
		return tupleDes;
	}

	/**
	 * dumps the schema on the standard output
	 */

	public void dump() {
		System.out.println("Schema :");
		for (int i = 0; i < tupleDes.size(); i++)
			((SchemaUnit) tupleDes.elementAt(i)).dump();
		System.out.println("---------------------------------");
	}
}
