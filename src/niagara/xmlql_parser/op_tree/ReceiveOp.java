/**
 * $Id: ReceiveOp.java,v 1.3 2002/05/23 06:32:03 vpapad Exp $
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

    public boolean isSourceOp() {
	return true;
    }
}

