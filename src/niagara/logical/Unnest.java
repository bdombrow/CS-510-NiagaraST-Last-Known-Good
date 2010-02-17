package niagara.logical;

import java.io.StringReader;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.path.RE;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.BaseAttr;
import niagara.xmlql_parser.REParser;
import niagara.xmlql_parser.Scanner;
import niagara.xmlql_parser.regExp;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;

public class Unnest extends UnaryOperator {
	/** Variable name of the result */
	private Attribute variable;
	/** atribute to unnest */
	private Attribute root;
	/** path to unnest */
	private RE path;
	/** The attributes we're projecting on (null means keep all attributes) */
	private Attrs projectedAttrs;

	/** keep or not tuples that do not have a match to the path expression */

	public enum OuterBehavior {
		NORMAL, /** Zero or more output tuples per input tuple, one per match */
		OUTER, /** Produce a null-padded output tuple even if there is no match */
		STRICT
		/** Check that there is at least one match per input tuple */
	};

	private OuterBehavior outer;

	public Unnest() {
	}

	public Unnest(Attribute variable, Attribute root, RE path,
			Attrs projectedAttrs, OuterBehavior outer) {
		this.variable = variable;
		this.root = root;
		this.path = path;
		this.projectedAttrs = projectedAttrs;
		this.outer = outer;
	}

	public Unnest(Unnest op) {
		this(op.variable, op.root, op.path, op.projectedAttrs, op.outer);
	}

	public Op opCopy() {
		return new Unnest(this);
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println(this);
	}

	public String toString() {
		return " unnest " + path + " from " + root.getName() + " into "
				+ variable.getName() + " project on "
				+ projectedAttrs.toString() + " include non matches "
				+ String.valueOf(outer);
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" regexp='").append(path).append("'");
		sb.append(" type='");
		sb.append(((NodeDomain) variable.getDomain()).getTypeDescription());
		sb.append("'");
		sb.append(" root='").append(root.getName()).append("'");
		sb.append(" non matches= ").append(String.valueOf(outer));
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();

		if (projectedAttrs == null)
			result.addAttr(variable);
		else
			result.setAttrs(projectedAttrs);

		// XXX vpapad: We don't have a way yet to estimate what the
		// cardinality will be, just use a global constant factor.
		result.setCardinality(input[0].getCardinality()
				* catalog.getInt("unnest_fanout"));
		return result;
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#projectedOutputAttributes(Attrs)
	 */
	public void projectedOutputAttributes(Attrs outputAttrs) {
		projectedAttrs = outputAttrs.copy();
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
	 */
	public Attrs requiredInputAttributes(Attrs inputAttrs) {
		// XXX vpapad: We always assume that regular expressions
		// cannot contain variable references...
		return new Attrs(root);
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof Unnest))
			return false;
		if (o.getClass() != Unnest.class)
			return o.equals(this);
		Unnest u = (Unnest) o;
		// XXX vpapad: regExp.equals is object.equals
		return variable.equals(u.variable) && root.equals(u.root)
				&& path.equals(u.path)
				&& equalsNullsAllowed(projectedAttrs, u.projectedAttrs)
				&& outer == u.outer;
	}

	public int hashCode() {
		return variable.hashCode() ^ root.hashCode() ^ path.hashCode()
				^ hashCodeNullsAllowed(projectedAttrs) ^ outer.hashCode();
	}

	/**
	 * Returns the path.
	 */
	public RE getPath() {
		return path;
	}

	/**
	 * Returns the root attribute.
	 */
	public Attribute getRoot() {
		return root;
	}

	/**
	 * Returns the variable.
	 */
	public Attribute getVariable() {
		return variable;
	}

	public Attrs getProjectedAttrs() {
		return projectedAttrs;
	}

	public OuterBehavior getOuter() {
		return outer;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String typeAttr = e.getAttribute("type");
		String rootAttr = e.getAttribute("root");
		String regexpAttr = e.getAttribute("regexp");
		String outerAttr = e.getAttribute("outer");
		String dataTypeAttr = e.getAttribute("datatype");

		if (outerAttr.equalsIgnoreCase("yes"))
			outer = OuterBehavior.OUTER;
		else if (outerAttr.equalsIgnoreCase("strict"))
			outer = OuterBehavior.STRICT;
		else
			outer = OuterBehavior.NORMAL;

		int type;
		if (typeAttr.equals("tag")) {
			type = varType.TAG_VAR;
		} else if (typeAttr.equals("element")) {
			type = varType.ELEMENT_VAR;
		} else { // (typeAttr.equals("content"))
			type = varType.CONTENT_VAR;
		}

		BaseAttr.Type dataType = BaseAttr.getDataTypeFromString(dataTypeAttr);

		// variable = new Variable(id, type);
		if (dataType == BaseAttr.Type.XML) {
			variable = new Variable(id, dataType, type);
		} else
			variable = new Variable(id, dataType);

		Scanner scanner;
		try {
			scanner = new Scanner(new StringReader(regexpAttr));
			REParser rep = new REParser(scanner);
			path = regExp.regExp2RE((regExp) rep.parse().value);
			rep.done_parsing();
		} catch (InvalidPlanException ipe) {
			String msg = ipe.getMessage();
			throw new InvalidPlanException(
					"Syntax error in regular expression for node " + "'"
							+ e.getAttribute("id") + "' " + msg);
		} catch (Exception ex) { // ugh cup throws "Exception!!!"
			ex.printStackTrace();
			throw new InvalidPlanException("Error while parsing: " + regexpAttr
					+ " in " + id);
		}

		LogicalProperty inputLogProp = inputProperties[0];

		if (rootAttr.length() > 0) {
			root = Variable.findVariable(inputLogProp, rootAttr);
		} else {
			// If root attr is left blank, we start the regexp from the last
			// attribute added to the input tuple
			Attrs attrs = inputLogProp.getAttrs();
			root = attrs.get(attrs.size() - 1);
		}
	}
}
