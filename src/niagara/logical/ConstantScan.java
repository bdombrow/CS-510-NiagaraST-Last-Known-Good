package niagara.logical;


import java.util.ArrayList;
import java.util.StringTokenizer;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/***
 * 
 * Constant is a pseudo operator that can be used to embed an XML document
 * in a query
 */
@SuppressWarnings("unchecked")
public class ConstantScan extends NullaryOperator {

    private Attrs vars;
    
    private String content;

    public ConstantScan() {}
    
    public ConstantScan(String content, Attrs vars) {
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
        ConstantScan cop = new ConstantScan();
        cop.content = this.content;
        cop.vars = this.vars.copy();
        return cop;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ConstantScan))
            return false;
        if (obj.getClass() != ConstantScan.class)
            return obj.equals(this);
        ConstantScan other = (ConstantScan) obj;
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

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
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
