/**
 * $Id: ResourceOp.java,v 1.5 2003/03/07 23:36:42 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.connection_server.NiagraServer;
import niagara.logical.NodeDomain;
import niagara.logical.NullaryOp;
import niagara.logical.Variable;
import niagara.optimizer.colombia.*;

public class ResourceOp extends NullaryOp {
    private Attribute variable;
    private String urn;

    // Required zero-argument constructor
    public ResourceOp() {
    }

    public ResourceOp(Attribute variable, String urn) {
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
        if (obj == null || !(obj instanceof ResourceOp))
            return false;
        if (obj.getClass() != ResourceOp.class)
            return obj.equals(this);
        ResourceOp other = (ResourceOp) obj;
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
    public Op copy() {
        return new ResourceOp(variable.copy(), urn);
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog cat, LogicalProperty[] input) {
        return new LogicalProperty(
            0,
            new Attrs(variable),
            isLocallyResolvable());
    }

    public boolean isLocallyResolvable() {
        return getCatalog().isLocallyResolvable(urn);
    }

    /**
     * Returns the catalog.
     * @return Catalog
     */
    public Catalog getCatalog() {
        return NiagraServer.getCatalog();
    }

    // XXX vpapad: I have to rethink this method...
    public boolean isSchedulable() {
        return false;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        this.variable =
            new Variable(e.getAttribute("id"), NodeDomain.getDOMNode());
        this.urn = e.getAttribute("urn");
    }
}
