/* $Id: Unnest.java,v 1.7 2003/07/03 19:39:02 tufte Exp $ */
package niagara.logical;

import org.w3c.dom.Element;

import java.io.StringReader;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.REParser;
import niagara.xmlql_parser.syntax_tree.Scanner;
import niagara.xmlql_parser.syntax_tree.regExp;
import niagara.xmlql_parser.syntax_tree.varType;

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

    public Op opCopy() {
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

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String typeAttr = e.getAttribute("type");
        String rootAttr = e.getAttribute("root");
        String regexpAttr = e.getAttribute("regexp");

        int type;
        if (typeAttr.equals("tag")) {
            type = varType.TAG_VAR;
        } else if (typeAttr.equals("element")) {
            type = varType.ELEMENT_VAR;
        } else { // (typeAttr.equals("content"))
            type = varType.CONTENT_VAR;
        }

        variable = new Variable(id, type);

        Scanner scanner;
        try {
            scanner = new Scanner(new StringReader(regexpAttr));
            REParser rep = new REParser(scanner);
            path  = (regExp) rep.parse().value;
            rep.done_parsing();
        } catch (Exception ex) { // ugh cup throws "Exception!!!"
            ex.printStackTrace();
            throw new InvalidPlanException(
                "Error while parsing: " + regexpAttr + " in " + id);
        }

        LogicalProperty inputLogProp = inputProperties[0];

        if (rootAttr.length() > 0) {
            root = Variable.findVariable(inputLogProp, rootAttr);
        } else {
            // If root attr is left blank, we start the regexp from the last
            // attribute added to the input tuple
            Attrs attrs = inputLogProp.getAttrs();
            root = attrs.get(attrs.size() - 1);
        }
    }
}
