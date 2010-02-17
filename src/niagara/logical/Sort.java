package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * This class is used to represent the Sort operator
 * 
 */

public class Sort extends UnaryOperator {
	// Constants used for comparison method
	public final static short ALPHABETIC_COMPARISON = 0;
	public final static short NUMERIC_COMPARISON = 1;

	/**
	 * Compare as numbers or strings
	 */
	private short comparisonMethod;

	private boolean ascending;

	/**
	 * The attribute we are sorting on
	 */
	private Attribute attr;

	/**
	 * @return the comparison method
	 */
	public short getComparisonMethod() {
		return comparisonMethod;
	}

	/**
	 * @return if the sort is ascending
	 */
	public boolean getAscending() {
		return ascending;
	}

	/**
	 * 
	 * @return the attribute we are sorting on
	 */
	public Attribute getAttr() {
		return attr;
	}

	/**
	 * used to configure the comparison method, and whether the sort is
	 * ascending or not
	 */
	public void setSort(Attribute attr, short comparisonMethod,
			boolean ascending) {
		this.attr = attr;
		this.comparisonMethod = comparisonMethod;
		this.ascending = ascending;
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Sort");
	}

	/**
	 * dummy toString method
	 * 
	 * @return String representation of this operator
	 */
	public String toString() {
		return "Sort";
	}

	/**
	 * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog,
	 *      LogicalProperty[])
	 */
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return input[0].copy();
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		Sort op = new Sort();
		op.setSort(attr, comparisonMethod, ascending);
		return op;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Sort))
			return false;
		if (obj.getClass() != Sort.class)
			return obj.equals(this);
		Sort other = (Sort) obj;
		return comparisonMethod == other.comparisonMethod
				&& ascending == other.ascending && attr.equals(other.attr);
	}

	public int hashCode() {
		int result = comparisonMethod ^ attr.hashCode();
		if (ascending)
			result = ~result;
		return result;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		LogicalProperty inputLogProp = inputProperties[0];

		String sortbyAttr = e.getAttribute("sort_by");
		Attribute sortBy = Variable.findVariable(inputLogProp, sortbyAttr);

		short comparisonMethod;
		String comparisonAttr = e.getAttribute("comparison");
		if (comparisonAttr.equals("alphabetic"))
			comparisonMethod = Sort.ALPHABETIC_COMPARISON;
		else
			comparisonMethod = Sort.NUMERIC_COMPARISON;

		boolean ascending;
		String orderAttr = e.getAttribute("order");
		ascending = !orderAttr.equals("descending");
		setSort(sortBy, comparisonMethod, ascending);
	}
}
