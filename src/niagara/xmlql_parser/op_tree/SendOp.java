/**
 * $Id: SendOp.java,v 1.2 2002/05/23 06:32:03 vpapad Exp $
 *
 */

/**
 * This operator is used to send results of a subplan to another
 * Niagara server.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.io.OutputStream;
import niagara.xmlql_parser.syntax_tree.*;

import niagara.connection_server.CommunicationServlet;

public class SendOp extends unryOp {
    String query_id;
    CommunicationServlet cs;

    public void setQueryId(String query_id) {
        this.query_id = query_id;
    }

    public String getQueryId() {
        return query_id;
    }

    public void setCS(CommunicationServlet cs) {
        this.cs = cs;
    }
    
    public CommunicationServlet getCS() {
        return cs;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Send op [" + query_id + "]");
    }

    /**
     * @return String representation of this operator
     */
    public String toString() {
        return "SendOp [" + query_id + "]";
    }
}
