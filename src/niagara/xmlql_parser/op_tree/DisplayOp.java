/**
 * This operator is used to send results of a subplan to another
 * Niagara server.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.io.OutputStream;
import niagara.xmlql_parser.syntax_tree.*;

public class DisplayOp extends unryOp {
    String query_id;
    String client_location;

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

    public String dumpAttributesInXML() {
        return "query_id='" + query_id + "' client_location='" + client_location + "' ";
    }
}
