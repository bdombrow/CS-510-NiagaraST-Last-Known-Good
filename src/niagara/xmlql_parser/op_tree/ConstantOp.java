/**
 * $Id: ConstantOp.java,v 1.2 2002/05/07 03:11:27 tufte Exp $
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

    /**
     * Constructor
     *
     * @param algList A list of algorithms which can be used to implement
     *      this operator.  Why is this passed as a parameter??
     */
    public ConstantOp(Class[] algList) {
	super("ConstantOp", algList);
    }

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


