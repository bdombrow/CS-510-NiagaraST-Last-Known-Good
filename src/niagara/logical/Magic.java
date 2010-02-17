package niagara.logical;

import java.util.ArrayList;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.predicates.Predicate;
import niagara.logical.predicates.True;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/***
 * The <code>Magic</code> operator sends feedback punctuation on time
 * intermittently
 * 
 * Magic operator RJFM 2008.09.23 Used to drive the CIDR query (simulates
 * sending feedback every fixed time interval.)
 * 
 * 
 * To do: (1) Clean up, since the operator is inherited from select and does not
 * need all methods (careful, I broke something on first try) (2) As with all FP
 * work, must actually send punctuation instead of a flattened message.
 * */

@SuppressWarnings("unchecked")
public class Magic extends UnaryOperator {

	/** Interval */
	private long interval;

	/** Attribute to punctuate on */
	private Attribute pAttr;

	/** Attribute to read time from */
	private Attribute tAttr;

	/** Number of tuples to allow through */
	private Predicate pred;

	/** Propagate? */
	private boolean propagate = false;

	public Magic() {
	}

	public Magic(Predicate pred, long interval, Attribute pAttr,
			Attribute tAttr, boolean propagate) {
		this.pred = pred;
		this.interval = interval;
		this.pAttr = pAttr;
		this.tAttr = tAttr;
		this.propagate = propagate;

	}

	public Magic(Magic selOp) {
		this(selOp.pred, selOp.interval, selOp.pAttr, selOp.tAttr,
				selOp.propagate);
	}

	public Op opCopy() {
		return new Magic(this);
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Magic :");
		System.out.println("interval = " + interval + " ");
		System.out.println("punctattr = " + pAttr + " ");
		System.out.println("timeattr = " + tAttr);
		System.out.println("propagate = " + propagate);
		pred.dump(1);

	}

	public String toString() {
		return " magic punctattr = " + pAttr.getName() + "timeattr = "
				+ tAttr.getName() + " interval = " + interval + pred.toString()
				+ " propagate = " + propagate;
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" ");
	}

	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		sb.append("punctattr = " + pAttr + " timeattr = " + tAttr
				+ " interval = " + interval + " propagate = " + propagate);
		pred.toXML(sb);
		sb.append("</magic>");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {

		String p = e.getAttribute("propagate");
		if (p.equals("yes"))
			propagate = true;

		String id = e.getAttribute("id");
		interval = Long.parseLong(e.getAttribute("interval"));

		String pAttrStr = e.getAttribute("punctattr");

		if (pAttrStr.length() == 0)
			throw new InvalidPlanException("Bad value for 'punctattr' for : "
					+ id);

		pAttr = Variable.findVariable(inputProperties[0], pAttrStr);

		String tAttrStr = e.getAttribute("timeattr");

		if (tAttrStr.length() == 0)
			throw new InvalidPlanException("Bad value for 'timeattr' for : "
					+ id);

		tAttr = Variable.findVariable(inputProperties[0], tAttrStr);

		NodeList children = e.getChildNodes();
		Element predElt = null;
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i) instanceof Element) {
				predElt = (Element) children.item(i);
				break;
			}
		}
		pred = Predicate.loadFromXML(predElt, inputProperties);
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();
		result.setCardinality(result.getCardinality() * pred.selectivity());
		return result;
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
	 */
	public Attrs requiredInputAttributes(Attrs inputAttrs) {
		ArrayList al = new ArrayList();
		pred.getReferencedVariables(al);
		Attrs reqd = new Attrs(al);
		assert inputAttrs.contains(reqd);
		return reqd;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof Magic))
			return false;
		if (o.getClass() != Magic.class)
			return o.equals(this);
		Magic s = (Magic) o;
		if (interval != s.interval)
			return false;
		if (pAttr.getName() != s.pAttr.getName())
			return false;
		if (tAttr.getName() != s.tAttr.getName())
			return false;
		if (propagate != s.propagate)
			return false;
		return pred.equals(s.pred);
	}

	public int hashCode() {
		return pred.hashCode();
	}

	public boolean getPropagate() {
		return propagate;
	}

	public Predicate getPredicate() {
		return pred;
	}

	public long getInterval() {
		return interval;
	}

	public Attribute getPunctAttr() {
		return pAttr;
	}

	public Attribute getTimeAttr() {
		return tAttr;
	}

	public boolean isEmpty() {
		return pred.equals(True.getTrue());
	}

	/* Can we combine the selection with this unnesting? */
	public boolean isPushableInto(Unnest unnest) {
		/*
		 * let's assume not ArrayList al = new ArrayList();
		 * pred.getReferencedVariables(al); return (al.size() == 1 &&
		 * ((Variable) al.get(0)).equals(unnest.getVariable()));
		 */
		return false;
	}

	/**
	 * used to set the predicate of Select operator. A list of predicate are
	 * ANDed to produce single predicate for this operator
	 * 
	 * @param list
	 *            of predicates KT - this is used by the trigger_engine *@#$*
	 */
	public void setSelect(Vector _preds) {
		pred = niagara.xmlql_parser.Util.andPredicates(_preds);
	}

}
