/**
 * This is the class for the max operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.logical;

import java.util.StringTokenizer;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.skolem;

import org.w3c.dom.Element;

public class SlidingSum extends SlidingWindow {
	// This is the attribute on which summing is done
	Attribute summingAttribute;
	int range;
	int every;

	/**
	 * This function sets the skolem attributes on which grouping is done, and
	 * the attribute that is summed
	 * 
	 * @param groupingAttrs
	 *            Attributes on which grouping is done
	 * @param summingAttribute
	 *            Attribute on which summing is done
	 */

	public void setSummingInfo(skolem groupingAttrs, Attribute summingAttribute) {

		// Set the summing attribute
		//
		this.summingAttribute = summingAttribute;

		// Set the skolem attributes in the super class
		//
		this.groupingAttrs = groupingAttrs;
	}

	/**
	 * This function returns the averaging attributes
	 * 
	 * @return Averaging attribute of the operator
	 */

	public Attribute getSummingAttribute() {
		return summingAttribute;
	}

	public void dump() {
		System.out.println("SlidingSumOp");
		groupingAttrs.dump();
		System.err.println(summingAttribute.getName());
	}

	public void setWindowInfo(int range, int every) {
		this.range = range;
		this.every = every;
	}

	public int getWindowRange() {
		return this.range;
	}

	public int getWindowEvery() {
		return this.every;
	}

	public Op opCopy() {
		SlidingSum op = new SlidingSum();
		op.setSummingInfo(groupingAttrs, summingAttribute);
		op.setWindowInfo(range, every);
		return op;
	}

	public int hashCode() {
		return groupingAttrs.hashCode() ^ summingAttribute.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof SlidingSum))
			return false;
		if (obj.getClass() != SlidingSum.class)
			return obj.equals(this);
		SlidingSum other = (SlidingSum) obj;
		return groupingAttrs.equals(other.groupingAttrs)
				&& summingAttribute.equals(other.summingAttribute);
	}

	@SuppressWarnings("unchecked")
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String sumattr = e.getAttribute("sumattr");
		String range = e.getAttribute("range");
		String every = e.getAttribute("every");

		// set the range and every parameter for the sliding window;
		//
		Integer rangeValue;
		Integer everyValue;
		if (range != "") {
			rangeValue = new Integer(range);
			if (rangeValue.intValue() <= 0)
				throw new InvalidPlanException("range must greater than zero");
		} else
			throw new InvalidPlanException("range ???");
		if (every != "") {
			everyValue = new Integer(every);
			if (everyValue.intValue() <= 0)
				throw new InvalidPlanException("every must greater than zero");
		} else
			throw new InvalidPlanException("every ???");

		setWindowInfo(rangeValue.intValue(), everyValue.intValue());

		LogicalProperty inputLogProp = inputProperties[0];

		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupby);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}

		Attribute summingAttribute = Variable.findVariable(inputLogProp,
				sumattr);
		setSummingInfo(new skolem(id, groupbyAttrs), summingAttribute);
	}
}
