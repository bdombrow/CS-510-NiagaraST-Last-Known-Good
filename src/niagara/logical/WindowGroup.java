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
public abstract class WindowGroup extends Group {
	// The attributes to group on (a.k.a. skolem attributes)
	protected skolem groupingAttrs;
	protected Attribute windowAttr;
	protected int windowType;
	protected int range;
	protected int slide;
	protected String widName;
	protected boolean propagate = false;
	protected boolean logging = false;
	protected Boolean exploit = false;
	
	public boolean getPropagate() {
		return propagate;
	}

	public boolean getExploit() {
		return exploit;
	}
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

		Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
		for (int i = 0; i < groupbyAttrs.size(); i++) {
			Attribute a = (Attribute) groupbyAttrs.get(i);
			attrs.add(a);
		}
		// and we add an attribute for the aggregated result
		attrs.add(new Variable(groupingAttrs.getName(), varType.CONTENT_VAR));
		// and we add an attribute for the TIMESTAMP, if it's a time-based
		// window
		// if (windowType == 1)
		// attrs.add(new Variable("TIMESTAMP", varType.CONTENT_VAR));
		// and we add an attribute for window id - don't need this because
		// wid_from is already a grouping attribute
		// attrs.add(new Variable("wid_from", varType.ELEMENT_VAR));

		return new LogicalProperty(card, attrs, inpLogProp.isLocal());
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
		String inputName = e.getAttribute("input");
		widName = e.getAttribute("wid");

		if (widName.equals(""))
			widName = inputName;

		// Parse the groupby attribute to see what to group on
		Vector groupbyAttrs = new Vector();
		StringTokenizer st = new StringTokenizer(groupbyStr);
		while (st.hasMoreTokens()) {
			String varName = st.nextToken();
			Attribute attr = Variable.findVariable(inputLogProp, varName);
			groupbyAttrs.addElement(attr);
		}
		// add the window_from (window_id) to the grouping attributes
		Attribute attr = Variable.findVariable(inputLogProp, "wid_from_"
				+ widName);
		groupbyAttrs.addElement(attr);
		groupingAttrs = new skolem(id, groupbyAttrs);
	}

	protected void loadWindowAttrsFromXML(Element e,
			LogicalProperty inputLogProp) throws InvalidPlanException {
		String propagateAttribute = e.getAttribute("propagate");

		if (propagateAttribute.equals("yes"))
			propagate = true;
		
		String exploitAttribute = e.getAttribute("exploit");

		if (exploitAttribute.equals("yes"))
			exploit = true;

		String windowAttribute = e.getAttribute("winattr");
		String type = e.getAttribute("wintype");
		String windowRange = e.getAttribute("range");
		String windowSlide = e.getAttribute("slide");

		if (type.length() == 0) {
			range = 0;
			slide = 0;
			windowType = -1;
		} else {
			windowType = new Integer(type).intValue();
			if (windowType == 0) {
				range = (Integer.valueOf(windowRange)).intValue();
				slide = (Integer.valueOf(windowSlide)).intValue();
			} else {
				range = Timer.parseTimeInterval(windowRange);
				slide = Timer.parseTimeInterval(windowSlide);
			}

			if (windowAttribute.length() != 0)
				windowAttr = Variable.findVariable(inputLogProp,
						windowAttribute);
		}
	}

	public Attribute getWindowAttr() {
		return windowAttr;
	}

	public int getWindowType() {
		return windowType;
	}

	public int getWindowRange() {
		return range;
	}

	public int getWindowSlide() {
		return slide;
	}

	public String getWid() {
		return widName;
	}
	
	public Boolean getLogging() {
		return logging;
	}

}
