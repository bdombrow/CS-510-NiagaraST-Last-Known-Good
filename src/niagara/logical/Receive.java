/**
 * $Id: Receive.java,v 1.1 2003/12/24 02:08:28 vpapad Exp $
 *
 */

/**
 * This operator is used to receive results from a subplan send to another
 * Niagara server.
 *
 */
package niagara.logical;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class Receive extends UnaryOperator {
    String location;
    String query_id;
    Send sop;
    LogicalProperty logProp;
    
    public Receive() {}

    public Receive(Send sop) {
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
        Receive rop = new Receive();
        rop.setReceive(location, query_id);
        return rop;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Receive)) return false;
        if (obj.getClass() != Receive.class) return obj.equals(this);
        Receive other = (Receive) obj;
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

