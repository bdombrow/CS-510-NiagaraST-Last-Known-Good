/**
 * $Id: ConstantOp.java,v 1.4 2002/10/27 01:20:21 vpapad Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

/**
 * ConstantOp is a pseudo operator that can be used to embed an XML document
 * in a query
 */

import java.util.*;

import niagara.logical.*;
import niagara.logical.NodeDomain;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

public class ConstantOp extends NullaryOp {

    private Attrs vars;
    
    private String content;


    public void setContent(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setVars(ArrayList vars) {
        this.vars = new Attrs(vars);
    }

    public void dump() {
        System.out.println("Constant Operator: ");
        System.out.println("Content: " + content);
    }

    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">").append(content).append("</constant>");
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        if (vars.size() > 0) {
            sb.append("vars='").append(((Variable) vars.get(0)).getName());
            for (int i = 1; i < vars.size(); i++) {
                sb.append(",").append(((Variable) vars.get(i)).getName());
            }
            sb.append("'");
        }
    }

    public boolean isSourceOp() {
        return true;
    }
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(1, vars, true);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        ConstantOp cop = new ConstantOp();
        cop.content = this.content;
        cop.vars = this.vars.copy();
        return cop;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ConstantOp))
            return false;
        if (obj.getClass() != ConstantOp.class)
            return obj.equals(this);
        ConstantOp other = (ConstantOp) obj;
        return content.equals(other.content) && vars.equals(other.vars);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return content.hashCode() ^ vars.hashCode();
    }
    /**
     * Returns the vars.
     * @return Attrs
     */
    public Attrs getVars() {
        return vars;
    }

}
