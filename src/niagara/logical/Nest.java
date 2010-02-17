package niagara.logical;

import java.io.StringReader;
import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.XMLUtils;
import niagara.xmlql_parser.ConstructParser;
import niagara.xmlql_parser.Scanner;
import niagara.xmlql_parser.attr;
import niagara.xmlql_parser.constructInternalNode;
import niagara.xmlql_parser.data;
import niagara.xmlql_parser.dataType;
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

// KT ooooh is this UUUGLY!!!!!!!
/**
 * This is the class for the nest operator, that is a type of group operator.
 * 
 * @version 1.0
 * 
 */
public class Nest extends Group {

	// This stores the template of the result
	// KT - we require a root node be specified in a nest construct template
	// that is <![CDATA[ $var ]]> is not a valid nest construct template
	private constructInternalNode resultTemplate;

	public Nest() {
		resultTemplate = null;
		groupingAttrs = null;
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		Nest newOp = new Nest();
		newOp.resultTemplate = this.resultTemplate;
		newOp.groupingAttrs = this.groupingAttrs;
		return newOp;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Nest))
			return false;
		if (obj.getClass() != Nest.class)
			return obj.equals(this);
		Nest other = (Nest) obj;
		// XXX vpapad: constructBaseNode.equals is still Object.equals
		return this.resultTemplate.equals(other.resultTemplate)
				&& this.groupingAttrs.equals(groupingAttrs);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return resultTemplate.hashCode() ^ groupingAttrs.hashCode();
	}

	/**
	 * This function sets the construct part of the nest operator, which
	 * specifies the result template
	 * 
	 * @param resultTemplate
	 *            The template of the result
	 */
	public void setNest(constructInternalNode resultTemplate) {
		// Set the result template
		this.resultTemplate = resultTemplate;
	}

	/**
	 * This function returns the result template associated with the operator
	 * 
	 * @return Result template associated with the operator
	 */
	public constructInternalNode getResTemp() {

		// Return the result template
		return resultTemplate;
	}

	public void dump() {
		System.out.println("Nest");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {

		NodeList children = e.getChildNodes();
		String content = "";
		for (int i = 0; i < children.getLength(); i++) {
			int nodeType = children.item(i).getNodeType();
			if (nodeType == Node.ELEMENT_NODE)
				content += XMLUtils.explosiveFlatten(children.item(i));
			else if (nodeType == Node.CDATA_SECTION_NODE)
				content += children.item(i).getNodeValue();
		}
		resultTemplate = null;

		try {
			Scanner scanner = new Scanner(new StringReader(content));
			ConstructParser cep = new ConstructParser(scanner);
			resultTemplate = (constructInternalNode) cep.parse().value;
			cep.done_parsing();
		} catch (InvalidPlanException ipe) {
			String msg = ipe.getMessage();
			throw new InvalidPlanException(
					"Syntax error in nest template for node " + "'"
							+ e.getAttribute("id") + "' " + msg);
		} catch (Exception ex) {
			throw new InvalidPlanException(
					"Error while parsing Nest construct template: " + content);
		}
		loadGroupingAttrsFromXML(e, inputProperties[0], "neston");
		verifyTopLevelAttrs();
	}

	// verify that only attributes used to nest are used in the
	// creation of attributes in the top-level node (this is
	// similar to restriction that only grouping attributes
	// can appear in the select clause of a query with groupby)
	@SuppressWarnings("unchecked")
	private void verifyTopLevelAttrs() // LogicalProperty inputLogProp)
			throws InvalidPlanException {
		// get attribute from the resultTemplate

		Vector attrs = resultTemplate.getStartTag().getAttrList();

		int numAttrs = attrs.size();
		for (int i = 0; i < numAttrs; i++) {
			attr attribute = (attr) attrs.get(i);
			data attrData = attribute.getValue();
			switch (attrData.getType()) {
			case dataType.STRING:
				// think ok - nothing to check
				break;
			case dataType.VAR:
				String varname = (String) attrData.getValue();
				if (varname.charAt(0) == '$')
					varname = varname.substring(1);
				Vector v = groupingAttrs.getVarList();
				boolean ok = false;
				for (int j = 0; j < v.size(); j++) {
					// v.get(j) is of type Variable
					String gpname = ((Variable) v.get(j)).getName();
					if (gpname.equals(varname))
						ok = true;
				}
				if (!ok)
					throw new InvalidPlanException(
							"Detected invalid top level attribute in Nest");
				break;

			default:
				// dont think I should get anything of type ATTR here
				assert false : "Unknown data type " + attrData.getType();
			}
		}
	}

	protected int getResultType() {
		return varType.ELEMENT_VAR;
	}
}
