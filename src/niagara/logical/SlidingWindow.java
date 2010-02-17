package niagara.logical;

import java.util.Vector;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.xmlql_parser.varType;

/**
 * This is the class for the logical group operator. This is an abstract class
 * from which various notions of grouping can be derived. The core part of this
 * class is the skolem function attributes that are used for grouping and are
 * common to all the sub-classes
 * 
 */
public abstract class SlidingWindow extends Group {
	/**
	 * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog,
	 *      LogicalProperty[])
	 */
	@SuppressWarnings("unchecked")
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty inpLogProp = input[0];
		// XXX vpapad: Really crude, fixed restriction factor for groupbys
		float card = inpLogProp.getCardinality()
				/ catalog.getInt("restrictivity");
		Vector groupbyAttrs = groupingAttrs.getVarList();
		// We keep the group-by attributes (possibly rearranged)
		// and we add an attribute for the aggregated result
		Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
		for (int i = 0; i < groupbyAttrs.size(); i++) {
			Attribute a = (Attribute) groupbyAttrs.get(i);
			attrs.add(a);
		}
		attrs.add(new Variable(groupingAttrs.getName(), varType.CONTENT_VAR));
		attrs.add(new Variable("index", varType.CONTENT_VAR));

		return new LogicalProperty(card, attrs, inpLogProp.isLocal());
	}
}
