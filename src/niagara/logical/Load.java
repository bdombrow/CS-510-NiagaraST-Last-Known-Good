package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/** Load a stream of XML documents out of local files in a binary SAXDOM format */
public class Load extends NullaryOperator {
	/** the attribute we create */
	private Attribute variable;
	/** The resource we're loading */
	private String resource;

	public Load() {
	}

	public Load(Attribute variable, String resource) {
		this.variable = variable;
		this.resource = resource;
	}

	public Load(Load op) {
		this(op.variable, op.resource);
	}

	public Op opCopy() {
		return new Load(this);
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		// XXX vpapad: We'll have to read statistics data from the catalog
		float card = 1;
		boolean local = true;

		return new LogicalProperty(card, new Attrs(variable), local);
	}

	public Attribute getVariable() {
		return variable;
	}

	public String getResource() {
		return resource;
	}

	public boolean isSourceOp() {
		return true;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Load))
			return false;
		if (obj.getClass() != Load.class)
			return obj.equals(this);
		Load other = (Load) obj;
		return equalsNullsAllowed(variable, other.variable)
				&& equalsNullsAllowed(resource, other.resource);
	}

	public int hashCode() {
		return hashCodeNullsAllowed(variable) ^ hashCodeNullsAllowed(resource);
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		variable = new Variable(e.getAttribute("id"), NodeDomain.getDOMNode());
		resource = e.getAttribute("resource");
		if (!resource.startsWith("urn:niagara:"))
			resource = "urn:niagara:" + resource;
		// The resource must be registered in the system catalog
		if (catalog.getFile(resource) == null)
			throw new InvalidPlanException("The resource " + resource
					+ " is not registered with the system catalog.");
	}
}
