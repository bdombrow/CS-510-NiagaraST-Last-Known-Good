/* $Id: Unnest.java,v 1.1 2002/10/06 23:40:13 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.optimizer.colombia.ATTR;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.regExp;

public class Unnest extends unryOp {
    /** Variable name of the result */
    private ATTR variable; 
    /** atribute to unnest */
    private ATTR  root; 
    /** path to unnest */
    private regExp path; 
    
    public Unnest() {}

    public Unnest(ATTR variable, ATTR root, regExp path) {
        this.variable = variable;
        this.root = root;
        this.path = path;
    }

    public Unnest(Unnest op) {
        this(op.variable, op.root, op.path);
    }

    public Op copy() {
        return new Unnest(this);
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println(this);
    }

    public String toString() {
        return " unnest " + path + " from " + root.getName() + " into " + variable.getName();
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" regexp='").append(path).append("'");
        sb.append(" type='").append(((NodeDomain) variable.getDomain()).getTypeDescription()).append("'");
        sb.append(" root='").append(root.getName()).append("'");
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();

        result.addAttr(variable);
        // XXX vpapad: We don't have a way yet to estimate what the 
        // cardinality will be, just use a global constant factor.
        result.setCardinality(input[0].getCardinality() * catalog.getInt("unnest_fanout"));
        return result;
    }
    
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Unnest))
            return false;
        if (o.getClass() != Unnest.class)
            return o.equals(this);
        Unnest u = (Unnest) o;
        // XXX vpapad: regExp.equals is object.equals
        return variable.equals(u.variable) 
               && root.equals(u.root)
               && path.equals(u.path);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        // XXX vpapad: need hashCode for regExp
        return variable.hashCode() ^ root.hashCode() ^ path.hashCode();
    }
    
    /**
     * Returns the path.
     * @return regExp
     */
    public regExp getPath() {
        return path;
    }

    /**
     * Returns the root attribute.
     * @return ATTR
     */
    public ATTR getRoot() {
        return root;
    }

    /**
     * Returns the variable.
     * @return ATTR
     */
    public ATTR getVariable() {
        return variable;
    }

}
