/* $Id: Unnest.java,v 1.5 2003/02/28 05:30:48 vpapad Exp $ */
package niagara.logical;

import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.regExp;

public class Unnest extends unryOp {
    /** Variable name of the result */
    private Attribute variable;
    /** atribute to unnest */
    private Attribute root;
    /** path to unnest */
    private regExp path;
    /** The attributes we're projecting on (null means keep all attributes) */
    private Attrs projectedAttrs;

    public Unnest() {
    }

    public Unnest(
        Attribute variable,
        Attribute root,
        regExp path,
        Attrs projectedAttrs) {
        this.variable = variable;
        this.root = root;
        this.path = path;
        this.projectedAttrs = projectedAttrs;
    }

    public Unnest(Unnest op) {
        this(op.variable, op.root, op.path, op.projectedAttrs);
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
        return " unnest "
            + path
            + " from "
            + root.getName()
            + " into "
            + variable.getName()
            + " project on "
            + projectedAttrs.toString();
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" regexp='").append(path).append("'");
        sb
            .append(" type='")
            .append(((NodeDomain) variable.getDomain()).getTypeDescription())
            .append("'");
        sb.append(" root='").append(root.getName()).append("'");
    }

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();

        if (projectedAttrs == null) 
            result.addAttr(variable);
        else
            result.setAttrs(projectedAttrs);

        // XXX vpapad: We don't have a way yet to estimate what the 
        // cardinality will be, just use a global constant factor.
        result.setCardinality(
            input[0].getCardinality() * catalog.getInt("unnest_fanout"));
        return result;
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#projectedOutputAttributes(Attrs)
     */
    public void projectedOutputAttributes(Attrs outputAttrs) {
        projectedAttrs = outputAttrs.copy();
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        // XXX vpapad: We always assume that regular expressions
        // cannot contain variable references...
        return new Attrs(root);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof Unnest))
            return false;
        if (o.getClass() != Unnest.class)
            return o.equals(this);
        Unnest u = (Unnest) o;
        // XXX vpapad: regExp.equals is object.equals
        return variable.equals(u.variable)
            && root.equals(u.root)
            && path.equals(u.path)
            && equalsNullsAllowed(projectedAttrs, u.projectedAttrs);
    }

    public int hashCode() {
        // XXX vpapad: need hashCode for regExp
        return variable.hashCode() ^ root.hashCode() ^ path.hashCode() ^ hashCodeNullsAllowed(projectedAttrs);
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
     */
    public Attribute getRoot() {
        return root;
    }

    /**
     * Returns the variable.
     */
    public Attribute getVariable() {
        return variable;
    }

    public Attrs getProjectedAttrs() {
        return projectedAttrs;
    }
}
