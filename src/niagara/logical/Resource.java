package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.connection_server.NiagraServer;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

public class Resource extends NullaryOperator {
	private Attribute variable;
	private String urn;

	// Required zero-argument constructor
	public Resource() {
	}

	public Resource(Attribute variable, String urn) {
		this.variable = variable;
		this.urn = urn;
	}

	public Attribute getVariable() {
		return variable;
	}

	public void setVariable(Attribute variable) {
		this.variable = variable;
	}

	public String getURN() {
		return urn;
	}

	public void setURN(String urn) {
		this.urn = urn;
	}

	/**
	 * print this operator to the standard output
	 */
	public void dump() {
		System.out.println("Resource: " + urn);
	}

	public String toString() {
		return "Resource: " + urn;
	}

	public void dumpAttributesInXML(StringBuffer sb) {
		sb.append(" urn='");
		sb.append(urn);
		sb.append("'");
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Resource))
			return false;
		if (obj.getClass() != Resource.class)
			return obj.equals(this);
		Resource other = (Resource) obj;
		return variable.equals(other.variable) && urn.equals(other.getURN());
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return variable.hashCode() ^ urn.hashCode();
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		return new Resource(variable.copy(), urn);
	}

	/**
	 * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog,
	 *      LogicalProperty[])
	 */
	public LogicalProperty findLogProp(ICatalog cat, LogicalProperty[] input) {
		return new LogicalProperty(0, new Attrs(variable),
				isLocallyResolvable());
	}

	public boolean isLocallyResolvable() {
		return getCatalog().isLocallyResolvable(urn);
	}

	/**
	 * Returns the catalog.
	 * 
	 * @return Catalog
	 */
	public Catalog getCatalog() {
		return NiagraServer.getCatalog();
	}

	// XXX vpapad: I have to rethink this method...
	public boolean isSchedulable() {
		return false;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		this.variable = new Variable(e.getAttribute("id"), NodeDomain
				.getDOMNode());
		this.urn = e.getAttribute("urn");
	}
}
