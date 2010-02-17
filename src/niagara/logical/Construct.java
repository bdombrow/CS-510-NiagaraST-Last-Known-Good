package niagara.logical;

import java.io.StringReader;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.XMLUtils;
import niagara.xmlql_parser.ConstructParser;
import niagara.xmlql_parser.Scanner;
import niagara.xmlql_parser.constructBaseNode;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This operator is used to construct XML results. This is analogous to SELECT
 * of SQL.
 * 
 */

public class Construct extends UnaryOperator {
	private Variable variable;

	constructBaseNode resultTemplate; // internal node or leaf node
	// if it is the internal node, then
	// all its children are leaf node that
	// represents the schemaAttributes

	/** The attributes we're projecting on (null means keep all attributes) */
	private Attrs projectedAttrs;

	public Construct() {
	}

	public Construct(Variable variable, constructBaseNode resultTemplate,
			Attrs projectedAttrs) {
		this.variable = variable;
		this.resultTemplate = resultTemplate;
		this.projectedAttrs = projectedAttrs;
	}

	public Construct(Construct op) {
		this(op.variable, op.resultTemplate, op.projectedAttrs);
	}

	public Op opCopy() {
		return new Construct(this);
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Construct))
			return false;
		if (obj.getClass() != Construct.class)
			return obj.equals(this);
		Construct other = (Construct) obj;
		return equalsNullsAllowed(variable, other.variable)
				&& equalsNullsAllowed(projectedAttrs, other.projectedAttrs)
				// XXX vpapad: constructBaseNode.equals is still Object.equals
				&& this.resultTemplate.equals(other.resultTemplate);
	}

	public int hashCode() {
		// XXX vpapad: constructBaseNode.hashCode is still Object.hashCode
		return hashCodeNullsAllowed(variable)
				^ hashCodeNullsAllowed(projectedAttrs)
				^ resultTemplate.hashCode();
	}

	/**
	 * @return the constructNode that has information about the tag names and
	 *         children
	 */
	public constructBaseNode getResTemp() {
		return resultTemplate;
	}

	/**
	 * used to set parameter for the construct operator
	 * 
	 * @param the
	 *            construct part (tag names and children if any)
	 */
	public void setConstruct(constructBaseNode temp) {
		resultTemplate = temp;
	}

	/**
	 * print the operator to the standard output
	 */
	public void dump() {
		System.out.println("Construct : ");
		resultTemplate.dump(1);
	}

	/**
	 * a dummy toString method
	 * 
	 * @return String representation of this operator
	 */
	public String toString() {
		return "ConstructOp";
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty result = input[0].copy();
		// XXX vpapad: We don't have a way yet to estimate what the
		// cardinality will be, assume same as input cardinality,
		// which is only true as long as you don't have skolems etc.
		if (projectedAttrs == null)
			result.addAttr(variable);
		else
			result.setAttrs(projectedAttrs);
		return result;
	}

	public void setVariable(Variable variable) {
		this.variable = variable;
	}

	/**
	 * Returns the variable.
	 * 
	 * @return Variable
	 */
	public Variable getVariable() {
		return variable;
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#projectedOutputAttributes(Attrs)
	 */
	public void projectedOutputAttributes(Attrs outputAttrs) {
		projectedAttrs = outputAttrs;
	}

	/**
	 * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
	 */
	public Attrs requiredInputAttributes(Attrs inputAttrs) {
		return resultTemplate.requiredInputAttributes(inputAttrs);
	}

	public Attrs getProjectedAttrs() {
		return projectedAttrs;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		variable = new Variable(e.getAttribute("id"));

		NodeList children = e.getChildNodes();
		String content = "";
		for (int i = 0; i < children.getLength(); i++) {
			int nodeType = children.item(i).getNodeType();
			if (nodeType == Node.ELEMENT_NODE)
				content += XMLUtils.explosiveFlatten(children.item(i));
			else if (nodeType == Node.CDATA_SECTION_NODE)
				content += children.item(i).getNodeValue();
		}

		Scanner scanner;
		resultTemplate = null;

		try {
			scanner = new Scanner(new StringReader(content));
			ConstructParser cep = new ConstructParser(scanner);
			resultTemplate = (constructBaseNode) cep.parse().value;
			cep.done_parsing();
			Attrs inputAttrs = inputProperties[0].getAttrs();
			Attrs usedVars = resultTemplate.requiredInputAttributes(inputAttrs);
			Attrs missingVars = new Attrs();
			for (int i = 0; i < usedVars.size(); i++) {
				String varName = usedVars.get(i).getName();
				if (inputAttrs.getAttr(varName) == null)
					missingVars.add(usedVars.get(i));
			}
			if (missingVars.size() != 0)
				throw new InvalidPlanException("Undeclared variable(s): "
						+ missingVars.toString());
		} catch (InvalidPlanException ipe) {
			String msg = ipe.getMessage();
			throw new InvalidPlanException(
					"Syntax error in construct template for node " + "'"
							+ e.getAttribute("id") + "' " + msg);
		} catch (Exception ex) {
			throw new InvalidPlanException("Error while parsing: " + content);
		}
	}
}
