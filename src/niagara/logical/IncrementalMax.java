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

public class IncrementalMax extends IncrementalGroup {
	private Attribute maxAttribute;
	private Double emptyGroupValue;

	public IncrementalMax() {
	}

	public IncrementalMax(skolem skolemAttributes, Attribute maxAttribute) {
		super(skolemAttributes);
		this.maxAttribute = maxAttribute;
	}

	public Double getEmptyGroupValue() {
		return emptyGroupValue;
	}

	public Attribute getMaxAttribute() {
		return maxAttribute;
	}

	public boolean outputOldValue() {
		return true;
	}

	public void dump() {
		System.out.println(getName());
	}

	public Op opCopy() {
		IncrementalMax op = new IncrementalMax(skolemAttributes, maxAttribute);
		op.emptyGroupValue = emptyGroupValue;
		return op;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof IncrementalMax))
			return false;
		if (o.getClass() != IncrementalMax.class)
			return o.equals(this);
		IncrementalMax ia = (IncrementalMax) o;
		return skolemAttributes.equals(ia.skolemAttributes)
				&& maxAttribute.equals(ia.maxAttribute);
	}

	public int hashCode() {
		return skolemAttributes.hashCode() ^ maxAttribute.hashCode();
	}

	@SuppressWarnings("unchecked")
	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupby = e.getAttribute("groupby");
		String maxattr = e.getAttribute("maxattr");
		String emptyGroupValueAttr = e.getAttribute("emptygroupvalue");

		LogicalProperty inputLogProp = inputProperties[0];

		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupby);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}

		maxAttribute = Variable.findVariable(inputLogProp, maxattr);
		setSkolemAttributes(new skolem(id, groupbyAttrs));
		emptyGroupValue = Double.valueOf(emptyGroupValueAttr);
	}
}
