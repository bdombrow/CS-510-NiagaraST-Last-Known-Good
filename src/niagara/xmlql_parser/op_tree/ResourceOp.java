/**
 * $Id: ResourceOp.java,v 1.4 2002/10/24 04:00:01 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

import java.util.*;

import niagara.connection_server.Catalog;
import niagara.logical.NullaryOp;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;

public class ResourceOp extends NullaryOp {
    private Attribute variable;
    private String urn;

    private Catalog catalog;

    // Required zero-argument constructor
    public ResourceOp() {
    }

    public ResourceOp(Attribute variable, String urn, Catalog catalog) {
        this.variable = variable;
        this.urn = urn;
        this.catalog = catalog;
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
        return new ResourceOp(variable.copy(), urn, catalog);
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog cat, LogicalProperty[] input) {
       return new LogicalProperty(0, new Attrs(variable), isLocallyResolvable());
    }

    public boolean isLocallyResolvable() {
        return catalog.isLocallyResolvable(urn);
    }

    /**
     * Returns the catalog.
     * @return Catalog
     */
    public Catalog getCatalog() {
        return catalog;
    }
}
