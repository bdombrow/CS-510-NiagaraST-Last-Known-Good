package niagara.logical;

import java.util.ArrayList;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.predicates.Predicate;
import niagara.logical.predicates.True;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The <code>Select</code> operator does a selection - so much for useful
 * comments!
 */
@SuppressWarnings("unchecked")
public class Select extends UnaryOperator {

	/** Number of tuples to allow through */
	private Predicate pred;

	public Select() {
	}

	public Select(Predicate pred) {
		this.pred = pred;
	}

	public Select(Select selOp) {
		this(selOp.pred);
	}

	public Op opCopy() {
		return new Select(this);
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Select :");
		pred.dump(1);
	}

	public String toString() {
		return " select " + pred.toString();
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" ");
	}

	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		pred.toXML(sb);
		sb.append("</select>");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {

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
		if (o == null || !(o instanceof Select))
			return false;
		if (o.getClass() != Select.class)
			return o.equals(this);
		Select s = (Select) o;
		return pred.equals(s.pred);
	}

	public int hashCode() {
		return pred.hashCode();
	}

	public Predicate getPredicate() {
		return pred;
	}

	public boolean isEmpty() {
		return pred.equals(True.getTrue());
	}

	/* Can we combine the selection with this unnesting? */
	public boolean isPushableInto(Unnest unnest) {
		ArrayList al = new ArrayList();
		pred.getReferencedVariables(al);
		return (al.size() == 1 && ((Variable) al.get(0)).equals(unnest
				.getVariable()));
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
