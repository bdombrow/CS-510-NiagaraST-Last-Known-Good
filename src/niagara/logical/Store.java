/* $Id: Store.java,v 1.1 2003/12/24 02:08:28 vpapad Exp $ */

package niagara.logical;

import org.w3c.dom.*;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

/** Store a parsed stream of documents into a local file, and  register
 * it as a resource with the system catalog. */
public class Store extends UnaryOperator {
    /** The variable whose document contents we're storing */
    private Attribute root;
    /** The name of the resource  we're creating */
    private String resource;

    public Store() {
    }

    public Store(Attribute root, String resource) {
        this.root = root;
        this.resource = resource;
    }

    public Store(Store op) {
        this(op.root, op.resource);
    }

    public Op opCopy() {
        return new Store(this);
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Store))
            return false;
        if (obj.getClass() != Store.class)
            return obj.equals(this);
        Store other = (Store) obj;
        return equalsNullsAllowed(root, other.root)
            && equalsNullsAllowed(resource, other.resource);
    }

    public int hashCode() {
        return hashCodeNullsAllowed(root) ^ hashCodeNullsAllowed(resource);
    }

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        // We don't really have any output
        return input[0];
    }

    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        return new Attrs(root);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        root = Variable.findVariable(inputProperties[0], e.getAttribute("root"));
        resource = e.getAttribute("resource");
        if (!resource.startsWith("urn:niagara:"))
            resource = "urn:niagara:" + resource; 
    }

    public String getResource() {
        return resource;
    }

    public Attribute getRoot() {
        return root;
    }
}
