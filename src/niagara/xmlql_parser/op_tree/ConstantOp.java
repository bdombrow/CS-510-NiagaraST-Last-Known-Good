/**
 * $Id: ConstantOp.java,v 1.6 2003/07/03 19:29:59 tufte Exp $
 *
 */

package niagara.xmlql_parser.op_tree;

/**
 * ConstantOp is a pseudo operator that can be used to embed an XML document
 * in a query
 */

import java.util.*;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.*;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.XMLUtils;

public class ConstantOp extends NullaryOp {

    private Attrs vars;
    
    private String content;

    public ConstantOp() {}
    
    public ConstantOp(String content, Attrs vars) {
            this.vars = vars;
            this.content = content;
    }

    public String getContent() {
        return content;
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
    public Op opCopy() {
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

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        content = "";
        NodeList children = e.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            content = content + XMLUtils.flatten(children.item(i), false);
        }

        String vars = e.getAttribute("vars");
        // Parse the vars attribute 
        // XXX vpapad: does this work?
        StringTokenizer st = new StringTokenizer(vars, ",");
        ArrayList variables = new ArrayList();
        while (st.hasMoreTokens()) {
            variables.add(new Variable(st.nextToken()));
        }

        if (variables.size() == 0) {
            variables.add(new Variable(id));
        }
        
        this.vars = new Attrs(variables);
    }
}
