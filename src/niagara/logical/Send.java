/**
 * $Id: Send.java,v 1.1 2003/12/24 02:08:27 vpapad Exp $
 *
 */

/**
 * This operator is used to send results of a subplan to another
 * Niagara server.
 *
 */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.CommunicationServlet;
import niagara.connection_server.InvalidPlanException;
import niagara.connection_server.Catalog;
import niagara.optimizer.colombia.LogicalProperty;

// XXX vpapad: hack to get CVS to compile
public class Send extends UnoptimizableLogicalOperator {
    String query_id;
    CommunicationServlet cs;

    String location;
    
    public Send() {}

    public Send(String location) {
        this.location = location;
    }
    
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

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
    }
}
