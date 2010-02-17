package niagara.logical;

import java.util.StringTokenizer;
import java.util.Vector;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.xmlql_parser.skolem;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

/**
 * This is the class for the logical group operator. This is an abstract class
 * from which various notions of grouping can be derived. The core part of this
 * class is the skolem function attributes that are used for grouping and are
 * common to all the sub-classes
 * 
 */
public abstract class Group extends UnaryOperator {
	// The attributes to group on (a.k.a. skolem attributes)
	protected skolem groupingAttrs;

	/**
	 * This function returns the skolem attributes associated with the group
	 * operator
	 * 
	 * @return The skolem attributes associated with the operator
	 */
	public skolem getSkolemAttributes() {
		return groupingAttrs;
	}

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

		int type = getResultType();
		attrs.add(new Variable(groupingAttrs.getName(), type));
		return new LogicalProperty(card, attrs, inpLogProp.isLocal());
	}

	// return type (element, content, tag) of the result
	protected int getResultType() {
		// this is the default - currently used for everything but nest
		return varType.CONTENT_VAR;
	}

	/**
	 * create the groupingAttrs object by loading the grouping attributes from
	 * an xml element. the grouping attributes are specified in an attribute of
	 * element e.
	 * 
	 * @param e
	 *            The element which contains the grouping attributes (as one of
	 *            its attributes)
	 * @param inputLogProp
	 *            logical properties of the input??
	 * @param gpAttrName
	 *            The attribute of element e which contains the grouping
	 *            attributes.
	 */
	@SuppressWarnings("unchecked")
	protected void loadGroupingAttrsFromXML(Element e,
			LogicalProperty inputLogProp, String gpAttrName)
			throws InvalidPlanException {
		String id = e.getAttribute("id");
		String groupbyStr = e.getAttribute(gpAttrName);

		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupbyStr);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}
		groupingAttrs = new skolem(id, groupbyAttrs);
	}
}
