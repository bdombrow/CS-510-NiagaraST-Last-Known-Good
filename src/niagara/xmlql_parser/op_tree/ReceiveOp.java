/**
 * $Id: ReceiveOp.java,v 1.6 2003/07/08 02:11:06 tufte Exp $
 *
 */

/**
 * This operator is used to receive results from a subplan send to another
 * Niagara server.
 *
 */
package niagara.xmlql_parser.op_tree;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class ReceiveOp extends unryOp {
    String location;
    String query_id;
    SendOp sop;
    LogicalProperty logProp;
    
    public ReceiveOp() {}

    public ReceiveOp(SendOp sop) {
        this.sop = sop;
    }        
       
    public void setReceive(String location, String query_id) {
        this.location = location;
        this.query_id = query_id;
    }

    public void setLogProp(LogicalProperty logProp) {
        this.logProp = logProp;
    }
    
    public String getLocation() {
        return location;
    }

    public String getQueryId() {
        return query_id;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Receive op [" + location + "@" + query_id + "]");
    }

    /**
     * @return String representation of this operator
     */
    public String toString() {
        return "ReceiveOp [" + location + "@" + query_id + "]";
    }

    public boolean isSourceOp() {
	return true;
    }
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        // For the logical model, receive is a NOP
        return input[0].copy();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        ReceiveOp rop = new ReceiveOp();
        rop.setReceive(location, query_id);
        return rop;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ReceiveOp)) return false;
        if (obj.getClass() != ReceiveOp.class) return obj.equals(this);
        ReceiveOp other = (ReceiveOp) obj;
        return location.equals(other.location) &&
               query_id.equals(other.query_id);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return location.hashCode() ^ query_id.hashCode();
    }
}

