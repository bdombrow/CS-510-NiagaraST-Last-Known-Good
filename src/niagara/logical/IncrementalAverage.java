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

public class IncrementalAverage extends IncrementalGroup {
	private Attribute avgAttribute;

	public IncrementalAverage() {
	}

	public IncrementalAverage(skolem skolemAttributes, Attribute avgAttribute) {
		super(skolemAttributes);
		this.avgAttribute = avgAttribute;
	}

	public Attribute getAvgAttribute() {
		return avgAttribute;
	}

	public void dump() {
		System.out.println(getName());
	}

	public Op opCopy() {
		return new IncrementalAverage(skolemAttributes, avgAttribute);
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof IncrementalAverage))
			return false;
		if (o.getClass() != IncrementalAverage.class)
			return o.equals(this);
		IncrementalAverage ia = (IncrementalAverage) o;
		return skolemAttributes.equals(ia.skolemAttributes)
				&& avgAttribute.equals(ia.avgAttribute);
	}

	public int hashCode() {
		return skolemAttributes.hashCode() ^ avgAttribute.hashCode();
	}

	@SuppressWarnings("unchecked")
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String avgattr = e.getAttribute("avgattr");
		LogicalProperty inputLogProp = inputProperties[0];

		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupby);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}

		avgAttribute = Variable.findVariable(inputLogProp, avgattr);
		setSkolemAttributes(new skolem(id, groupbyAttrs));
	}
}
