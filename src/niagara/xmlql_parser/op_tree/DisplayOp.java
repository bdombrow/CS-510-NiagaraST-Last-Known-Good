/**
 * This operator is used to send results of a subplan to another
 * Niagara server.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.io.OutputStream;
import java.util.ArrayList;

import niagara.xmlql_parser.syntax_tree.*;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class DisplayOp extends unryOp {
    String query_id;
    String client_location;

    public DisplayOp(){}
    
    public DisplayOp(String query_id, String client_location) {
        this.query_id = query_id;
        this.client_location = client_location;
    }
    
    public DisplayOp(DisplayOp op) {
        this(op.query_id, op.client_location);    
    }
        
    public Op copy() {
        return new DisplayOp(this);
    }
    
    public void setQueryId(String query_id) {
        this.query_id = query_id;
    }

    public String getQueryId() {
        return query_id;
    }

    public void setClientLocation(String client_location) {
        this.client_location = client_location;
    }
    
    public String getClientLocation() {
        return client_location;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Display op [" + query_id + "@" 
                           + client_location + "]");
    }

    /**
     * @return String representation of this operator
     */
    public String toString() {
        return "Display op [" + query_id + "@" 
            + client_location + "]";
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append("query_id='").append(query_id);
        sb.append("' client_location='").append(client_location).append("' ");
    }
    
    public LogicalProperty findLogProp (ICatalog catalog, LogicalProperty[] input) {
        return input[0].copy();
    }
    
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof DisplayOp)) return false;
        if (obj.getClass() != DisplayOp.class) return obj.equals(this);
        DisplayOp other = (DisplayOp) obj;
        return query_id.equals(other.query_id) && client_location.equals(other.client_location);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return query_id.hashCode() ^ client_location.hashCode();
    }

}
