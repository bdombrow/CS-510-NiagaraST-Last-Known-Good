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

/**
 * This is the class for the max operator, that is a type of group operator.
 * 
 * @version 1.0
 * 
 */

public class SlidingMax extends SlidingWindow {

	// This is the attribute on which maxing is done
	Attribute maxingAttribute;
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

	public void setMaxingInfo(skolem groupingAttrs, Attribute maxingAttribute) {

		// Set the maxing attribute
		//
		this.maxingAttribute = maxingAttribute;

		// Set the skolem attributes in the super class
		//
		this.groupingAttrs = groupingAttrs;
	}

	/**
	 * This function returns the averaging attributes
	 * 
	 * @return Maxing attribute of the operator
	 */

	public Attribute getMaxingAttribute() {
		return maxingAttribute;
	}

	public void dump() {
		System.out.println("SlidingMaxOp");
		groupingAttrs.dump();
		System.err.println(maxingAttribute.getName());
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
		SlidingMax op = new SlidingMax();
		op.setMaxingInfo(groupingAttrs, maxingAttribute);
		op.setWindowInfo(range, every);
		return op;
	}

	public int hashCode() {
		return groupingAttrs.hashCode() ^ maxingAttribute.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof SlidingMax))
			return false;
		if (obj.getClass() != SlidingMax.class)
			return obj.equals(this);
		SlidingMax other = (SlidingMax) obj;
		return groupingAttrs.equals(other.groupingAttrs)
				&& maxingAttribute.equals(other.maxingAttribute);
	}

	@SuppressWarnings("unchecked")
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String maxattr = e.getAttribute("maxattr");
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

		Attribute maxingAttribute = Variable
				.findVariable(inputLogProp, maxattr);
		setMaxingInfo(new skolem(id, groupbyAttrs), maxingAttribute);
	}
}
