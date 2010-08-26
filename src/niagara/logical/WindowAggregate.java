package niagara.logical;

import java.util.ArrayList;

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
@SuppressWarnings("unchecked")
public abstract class WindowAggregate extends WindowGroup {

	//private Boolean logging = false;
	// the attribute on which the aggregation function should
	// be performed
	protected ArrayList<Attribute> aggrAttr = new ArrayList();

	protected abstract WindowAggregate getInstance();

	protected void loadFromXML(Element e, LogicalProperty[] inputProperties,
			ArrayList<String> aggrAttrName) throws InvalidPlanException {

		for (String name : aggrAttrName) {
			String aggAttrStr = e.getAttribute(name);
			aggrAttr.add(Variable.findVariable(inputProperties[0], aggAttrStr));
		}
		
		loadGroupingAttrsFromXML(e, inputProperties[0], "groupby");
		loadWindowAttrsFromXML(e, inputProperties[0]);
		
		String logAttribute = e.getAttribute("log");

		if (logAttribute.equals("yes"))
			logging = true;
		
		fAttr = e.getAttribute("fattrs");
		
	}

	public ArrayList getAggrAttr() {
		return aggrAttr;
	}

	public boolean getPropagate() {
		return propagate;
	}
	
	public boolean getExploit() {
		return exploit;
	}

	public Boolean getLogging() {
		return logging;
	}


	protected void dump(String opName) {
		System.out.println("opName");
		System.out.print("Grouping Attrs: ");
		groupingAttrs.dump();
		System.out.println("Propagate: " + propagate);
		System.err.print("Aggregate Attr: ");
		for (Attribute attr : aggrAttr) {
			System.err.print(attr.getName() + " ");
		}
		System.err.println(" ");
	}

	public Op opCopy() {
		WindowAggregate op = null;
		op = getInstance();
		op.groupingAttrs = this.groupingAttrs;
		op.aggrAttr = this.aggrAttr;
		op.widName = widName;
		op.propagate = propagate;
		op.logging = logging;
		op.exploit = exploit;
		op.fAttr = fAttr;
		
		return op;
	}

	public int hashCode() {
		int p = 0, l = 0, e =0;
		if (propagate)
			p = 1;
		if (logging)
			l = 1;
		if (exploit)
			e = 1;
		
		return groupingAttrs.hashCode() ^ aggrAttr.hashCode()
				^ widName.hashCode() ^ p ^ l ^ e;
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!this.getClass().isInstance(obj))
			return false;
		if (obj.getClass() != this.getClass())
			return obj.equals(this);

		WindowAggregate other = (WindowAggregate) obj;
		if (propagate == other.propagate && logging == other.logging && exploit == other.exploit) {
			return groupingAttrs.equals(other.groupingAttrs)
					&& aggrAttr.equals(other.aggrAttr)
					&& widName.equals(other.widName);
		}
		return false;
	}

}
