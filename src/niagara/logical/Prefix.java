/* $Id: Prefix.java,v 1.2 2003/07/03 19:39:02 tufte Exp $ */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.unryOp;

/** The <code>Prefix</code> operator's output is a finite prefix of
 * its input stream */
public class Prefix extends unryOp {

    /** Number of tuples to allow through */
    private int length;

    public Prefix() {
    }

    public Prefix(int length) {
        this.length = length;
    }

    public Prefix(Prefix op) {
        this(op.length);
    }

    public Op opCopy() {
        return new Prefix(this);
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println(this);
    }

    public String toString() {
        return " prefix " + length;
    }

    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" length='").append(length).append("'");
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String lengthStr = e.getAttribute("length");
        try {
            length = Integer.parseInt(lengthStr);
        } catch (NumberFormatException nfe) {
            throw new InvalidPlanException(
                "Expected integer, found "
                    + lengthStr
                    + " while parsing "
                    + e.getAttribute("id"));
        }
    }

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        if (result.getCardinality() > length)
            result.setCardinality(length);
        return result;
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        return new Attrs();
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof Prefix))
            return false;
        if (o.getClass() != Prefix.class)
            return o.equals(this);
        Prefix u = (Prefix) o;
        return u.length == length;
    }

    public int hashCode() {
        return length;
    }

    public int getLength() {
        return length;
    }
}
