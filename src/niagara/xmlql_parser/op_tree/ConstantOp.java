/**
 * $Id: ConstantOp.java,v 1.3 2002/05/23 06:32:03 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

/**
 * ConstantOp is a pseudo operator that can be used to embed an XML document
 * in a query
 */

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class ConstantOp extends unryOp {

    private String content;
    
    private String vars;

    public void setContent(String content) {
	this.content = content;
    }

    public String getContent() {
	return content;
    }

    public void setVars(String vars) {
        this.vars = vars;
    }

    public void dump() {
	System.out.println("Constant Operator: ");
	System.out.println("Content: " + content);
    }

    public String dumpChildrenInXML() {
        return content;
    }
    
    public String dumpAttributesInXML() {
        return vars;
    }

    public boolean isSourceOp() {
	return true;
    }
}


