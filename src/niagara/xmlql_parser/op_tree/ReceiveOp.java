/**
 * $Id: ReceiveOp.java,v 1.1 2001/07/17 06:52:23 vpapad Exp $
 *
 */

/**
 * This operator is used to receive results of a subplan to another
 * Niagara server.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.io.OutputStream;
import niagara.xmlql_parser.syntax_tree.*;

public class ReceiveOp extends unryOp {
    String location;
    String query_id;

    /**
     * Constructor
     *
     * @param list of algorithm to implement this operator
     */
    public ReceiveOp(Class[] al) {
        super(new String("Receive"), al);
    }
    
    public void setReceive(String location, String query_id) {
        this.location = location;
        this.query_id = query_id;
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
}