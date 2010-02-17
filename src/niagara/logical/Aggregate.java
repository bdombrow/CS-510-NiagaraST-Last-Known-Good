package niagara.logical;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * This is the class for the logical group operator. This is an abstract class
 * from which various notions of grouping can be derived. The core part of this
 * class is the skolem function attributes that are used for grouping and are
 * common to all the sub-classes
 * 
 */
public abstract class Aggregate extends Group {

	// the attribute on which the aggregation function should
	// be performed
	protected Attribute aggrAttr;

	protected abstract Aggregate getInstance();

	protected void loadFromXML(Element e, LogicalProperty[] inputProperties,
			String aggrAttrName) throws InvalidPlanException {

		String aggAttrStr = e.getAttribute(aggrAttrName);
		aggrAttr = Variable.findVariable(inputProperties[0], aggAttrStr);
		loadGroupingAttrsFromXML(e, inputProperties[0], "groupby");
	}

	public Attribute getAggrAttr() {
		return aggrAttr;
	}

	protected void dump(String opName) {
		System.out.println("opName");
		System.out.print("Grouping Attrs: ");
		groupingAttrs.dump();
		System.err.println("Aggregate Attr: " + aggrAttr.getName());
	}

	public Op opCopy() {
		Aggregate op = null;
		op = getInstance();
		op.groupingAttrs = this.groupingAttrs;
		op.aggrAttr = this.aggrAttr;
		return op;
	}

	public int hashCode() {
		return groupingAttrs.hashCode() ^ aggrAttr.hashCode();
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!this.getClass().isInstance(obj))
			return false;
		if (obj.getClass() != this.getClass())
			return obj.equals(this);
		Aggregate other = (Aggregate) obj;
		return groupingAttrs.equals(other.groupingAttrs)
				&& aggrAttr.equals(other.aggrAttr);
	}
}
