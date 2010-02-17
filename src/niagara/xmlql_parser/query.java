package niagara.xmlql_parser;

import java.util.ArrayList;
import java.util.Vector;

import niagara.logical.predicates.Predicate;

/**
 * This class is used to represent the base XML-QL query.
 * 
 */
@SuppressWarnings("unchecked")

public class query {

	// list of conditions that corresponds to comma separated predicates,
	// and IN clause (including the set IN) in the WHERE part of the 
	// query

	private Vector condList;

	// represents the construct part (nested queries ignored !!!!)

	private constructBaseNode constructPart;

	/**
	 * Constructor
         *
	 * @param list of conditions (set, predicates, IN clause)
	 * @param the construct part
	 */

	public query(Vector v, constructBaseNode q) {
		condList = v;
		constructPart = q;
	}

	/**
	 * @returns the list of conditions
	 */

	public Vector getConditions() {
		return condList;
	}

	/**
	 * @returns the construct part
	 */

	public constructBaseNode getConstructPart() {
		return constructPart;
	}

	/**
	 * prints the query on the standard output
	 */

	public void dump() {
		//condition cond;
		//Vector varList;
		String var;

		System.out.println("Query:");

		// prints each condition
		for(int i=0; i<condList.size(); i++){
                        Object oc = condList.elementAt(i);
			if(oc instanceof Predicate) {
				ArrayList al = new ArrayList();
				((Predicate)oc).getReferencedVariables(al);
				System.out.print("list of predicate var: ");
				for(int j=0;j<al.size();j++) {
					var = (String)al.get(j);
					System.out.print(var+ " ");
				}
				System.out.println();
                                ((Predicate) oc).dump(0);
			}
                        else {
        			((condition) oc).dump(0);
                        }
		}

		// prints the construct part
		System.out.println("Construct Part");
		if(constructPart instanceof constructInternalNode)
			((constructInternalNode)constructPart).dump(0);
		else
			((constructLeafNode)constructPart).dump(0);
	}
}
