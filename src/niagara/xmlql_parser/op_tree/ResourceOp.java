/**
 * $Id: ResourceOp.java,v 1.1 2001/07/17 06:52:23 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class ResourceOp extends unryOp {

    private String urn; 

    /**
     * Constructor
     *
     * @param list of algorithms used to implement this operator
     */
    public ResourceOp(Class[] al) {
	super("Resource", al);
    }
  
    public String getURN() {
	return urn;
    }

    public void setURN (String urn) {
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

    public String dumpAttributesInXML() {
        return " urn='" + urn  +"'";
    }
}
