/**
 * $Id: ResourceOp.java,v 1.2 2002/05/23 06:32:03 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class ResourceOp extends unryOp {

    private String urn; 

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
