package niagara.logical;

import java.util.Vector;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.xmlql_parser.skolem;

public abstract class PartitionGroup extends UnaryOperator {
	// The skolem attributes associated with the group operator
	protected skolem skolemAttributes;

	public PartitionGroup() {
	}

	public PartitionGroup(skolem skolemAttributes) {
		this.skolemAttributes = skolemAttributes;
	}

	public void setSkolemAttributes(skolem skolemAttributes) {
		this.skolemAttributes = skolemAttributes;
	}

	public skolem getSkolemAttributes() {
		return skolemAttributes;
	}

	/** Does the output schema of this operator include the old group value? */
	public boolean outputOldValue() {
		return false;
	}

	public abstract Boolean landmark();

	public abstract Integer getRange();

	@SuppressWarnings("unchecked")
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty inpLogProp = input[0];
		// XXX vpapad: Really crude, we assume incremental groupbys
		// have the same cardinality as their input
		float card = inpLogProp.getCardinality();
		Vector groupbyAttrs = skolemAttributes.getVarList();
		// We keep the group-by attributes (possibly rearranged)
		// and we add an attribute for the aggregated result
		Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
		for (int i = 0; i < groupbyAttrs.size(); i++) {
			Attribute a = (Attribute) groupbyAttrs.get(i);
			attrs.add(a);
		}
		if (outputOldValue())
			attrs.add(new Variable("old" + skolemAttributes.getName()));
		attrs.add(new Variable(skolemAttributes.getName()));

		return new LogicalProperty(card, attrs, inpLogProp.isLocal());
	}
}
