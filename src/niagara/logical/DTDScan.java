package niagara.logical;

import java.util.Vector;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.rules.Initializable;
import niagara.xmlql_parser.data;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class represent dtd Scan operator that fetches the data sources, parses
 * it and then returns the DOM tree to the operator above it.
 * 
 */
@SuppressWarnings("unchecked")
public class DTDScan extends NullaryOperator implements Initializable {
	/** the attribute we create */
	private Attribute variable;
	private Vector docs;// Vector of urls to scan.
	private String type;// dtd name (e.g book.dtd)

	public DTDScan() {
	}

	public DTDScan(Attribute variable, Vector docs, String type) {
		this.variable = variable;
		this.docs = docs;
		this.type = type;
	}

	public DTDScan(DTDScan op) {
		this(op.variable, op.docs, op.type);
	}

	public Op opCopy() {
		return new DTDScan(this);
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		LogicalProperty lp = null;

		float card = 0;
		boolean local = true;

		// Cardinality is the sum of document cardinalities,
		// locality is the conjunction of document localities
		for (int i = 0; i < docs.size(); i++) {
			lp = catalog.getLogProp((String) docs.get(i));
			card += lp.getCardinality();
			local |= lp.isLocal();
		}

		return new LogicalProperty(card, new Attrs(variable), local);
	}

	/** Initialize the dtdscan from a resource operator */
	public void initFrom(LogicalOp op) {
		Resource rop = (Resource) op;
		docs = new Vector();
		docs.addAll(rop.getCatalog().getURL(rop.getURN()));
		variable = rop.getVariable();
	}

	/**
	 * @return list of XML data sources (either URLs or local files)
	 */
	public Vector getDocs() {
		return docs;
	}

	/**
	 * This function sets the vector of documents associated with the dtd scan
	 * operator
	 * 
	 * @param docVector
	 *            The set of documents associated with the operator
	 */

	public void setDocs(Vector docVector) {
		docs = docVector;
	}

	public Attribute getVariable() {
		return variable;
	}

	/**
	 * @return DTD type of the XML sources to query
	 */
	public String getDtdType() {
		return type;
	}

	/**
	 * sets the parameters for this operator
	 * 
	 * @param list
	 *            of XML data sources
	 * @param DTD
	 *            type of these sources
	 */
	public void setDtdScan(Vector v, String s) {
		docs = v;
		type = s;
	}

	/**
	 * prints this operator to the standard output
	 */
	public void dump() {
		System.out.println("DtdScan :");
		for (int i = 0; i < docs.size(); i++) {
			Object o = docs.elementAt(i);
			if (o instanceof String)
				System.out.println("\t" + (String) o);
			else if (o instanceof data)
				((data) o).dump();
		}
		System.out.println("\t" + type);
	}

	/**
	 * dummy toString method
	 * 
	 * @return String representation of this operator
	 */
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append("Data Scan");
		return strBuf.toString();
	}

	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		for (int i = 0; i < docs.size(); i++)
			sb.append("<url value='").append(docs.elementAt(i)).append("'/>");
		sb.append("</dtdscan>");
	}

	public boolean isSourceOp() {
		return true;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof DTDScan))
			return false;
		if (obj.getClass() != DTDScan.class)
			return obj.equals(this);
		DTDScan other = (DTDScan) obj;
		if ((type == null) != (other.type == null))
			return false;
		if (type != null && !type.equals(other.type))
			return false;
		return variable.equals(other.variable) && docs.equals(other.docs);
	}

	public int hashCode() {
		int hashCode = 0;
		if (variable != null)
			hashCode ^= variable.hashCode();
		if (docs != null)
			hashCode ^= docs.hashCode();
		if (type != null)
			hashCode ^= type.hashCode();
		return hashCode;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");

		// The node's children contain URLs
		Vector urls = new Vector();
		NodeList children = ((Element) e).getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			if (child.getNodeType() != Node.ELEMENT_NODE)
				continue;
			urls.addElement(((Element) child).getAttribute("value"));
		}

		setDocs(urls);
		variable = new Variable(id, NodeDomain.getDOMNode());
	}

}
