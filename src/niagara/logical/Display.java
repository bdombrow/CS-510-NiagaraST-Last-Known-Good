/**
 * This operator is used to send results of a subplan to another
 * Niagara server.
 *
 */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class Display extends UnaryOperator {
    String query_id;
    String client_location;

    public Display() {
    }

    public Display(String query_id, String client_location) {
        this.query_id = query_id;
        this.client_location = client_location;
    }

    public Display(Display op) {
        this(op.query_id, op.client_location);
    }

    public Op opCopy() {
        return new Display(this);
    }

    public String getQueryId() {
        return query_id;
    }

    public String getClientLocation() {
        return client_location;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println(
            "Display op [" + query_id + "@" + client_location + "]");
    }

    /**
     * @return String representation of this operator
     */
    public String toString() {
        return "Display op [" + query_id + "@" + client_location + "]";
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append("query_id='").append(query_id);
        sb.append("' client_location='").append(client_location).append("' ");
    }

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        return input[0].copy();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) {
        query_id = e.getAttribute("query_id");
        client_location = e.getAttribute("client_location");
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        // XXX vpapad: does this work?!
        return new Attrs(inputAttrs.get(0));
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Display))
            return false;
        if (obj.getClass() != Display.class)
            return obj.equals(this);
        Display other = (Display) obj;
        return query_id.equals(other.query_id)
            && client_location.equals(other.client_location);
    }

    public int hashCode() {
        return query_id.hashCode() ^ client_location.hashCode();
    }
}
