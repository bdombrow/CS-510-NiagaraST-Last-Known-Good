/* $Id */
package niagara.logical.predicates;

import java.util.ArrayList;

import niagara.physical.predicates.OrImpl;
import niagara.physical.predicates.PredicateImpl;

/** Disjunction of two predicates */
public class Or extends BinaryPredicate {
    Predicate left, right;

    public Or(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        return new OrImpl(left.getImplementation(), right.getImplementation());
    }
    

    public void beginXML(StringBuffer sb) {
        sb.append("<or>");
    }
    
    public void childrenInXML(StringBuffer sb) {
        left.toXML(sb);
        right.toXML(sb);
    }
    
    public void endXML(StringBuffer sb) {
        sb.append("</or>");
    }
    
    /**
     * @see niagara.logical.Predicate#getReferencedVariables(ArrayList)
     */
    public void getReferencedVariables(ArrayList al) {
        left.getReferencedVariables(al);
        right.getReferencedVariables(al);
    }

    /**
     * @see niagara.logical.BinaryPredicate#getLeft()
     */
    public Predicate getLeft() {
        return left;
    }

    /**
     * @see niagara.logical.BinaryPredicate#getRight()
     */
    public Predicate getRight() {
        return right;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Or))
            return false;
        if (other.getClass() != Or.class)
            return other.equals(this);
        Or or = (Or) other;
        return left.equals(or.getLeft()) && right.equals(or.getRight());
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return left.hashCode()^ right.hashCode();
    }

    /**
     * @see niagara.logical.Predicate#negate()
     */
    Predicate negation() {
        return new And(left.negation(), right.negation());
    }

    /**
     * @see niagara.logical.Predicate#selectivity()
     */
    public float selectivity() {
        // Independence assumption
        float l = left.selectivity();
        float r = right.selectivity();
        return l + r - l*r;
    }
}
