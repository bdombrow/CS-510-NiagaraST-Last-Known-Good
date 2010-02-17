
/**
 * This operator is used to read a regular expression from the root of a given
 * subtree.
 *
 */
package niagara.logical;

import niagara.xmlql_parser.regExp;
import niagara.xmlql_parser.schemaAttribute;

public class ScanOp extends UnoptimizableLogicalOperator {

	private schemaAttribute attrId; // represents the root of the subtree
	// or one can call it the ancestor of the
	// element that is to be scanned
	private regExp regExpToScan; // paths to the elements to scan

	/**
	 * get the schemaAttribute that represent the root of the subtree at which
	 * the regularExpression representing the path starts.
	 * 
	 * @return the ancestor of the elements to scan
	 */
	public schemaAttribute getParent() {
		return attrId;
	}

	/**
	 * @return the path of the elements to scan
	 */
	public regExp getRegExpToScan() {
		return regExpToScan;
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Scan:");
		attrId.dump(1);
		regExpToScan.dump(1);
	}

	/**
	 * dummy toString method
	 * 
	 * @return String representation of the operator
	 */
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("Scan");

		return strBuf.toString();
	}

	/**
	 * set the parameters for this operator
	 * 
	 * @param the
	 *            root of the subtree at which the regular expression starts
	 * @param the
	 *            path of the elements to scan
	 */
	public void setScan(schemaAttribute parent, regExp toScan) {
		attrId = parent;
		regExpToScan = toScan;
	}
}
