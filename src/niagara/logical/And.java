/* $Id: And.java,v 1.3 2003/09/16 04:53:36 vpapad Exp $ */
package niagara.logical;

import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.AndImpl;
import niagara.query_engine.PredicateImpl;

/** Conjunction of two predicates */
public class And extends BinaryPredicate {
    Predicate left, right;

    public And(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        return new AndImpl(left.getImplementation(), right.getImplementation());
    }

    /**
     * @see niagara.logical.Predicate#getReferencedVariables(ArrayList)
     */
    public void getReferencedVariables(ArrayList al) {
        left.getReferencedVariables(al);
        right.getReferencedVariables(al);
    }
    /**
     * Returns the left side of the conjunction.
     * @return Predicate
     */
    public Predicate getLeft() {
        return left;
    }

    /**
     * Returns the right side of the conjunction.
     * @return Predicate
     */
    public Predicate getRight() {
        return right;
    }

    public And split(Attrs variables) {
        And leftSplit = left.split(variables);
        Predicate l1 = leftSplit.getLeft();
        Predicate l2 = leftSplit.getRight();

        And rightSplit = right.split(variables);
        Predicate r1 = rightSplit.getLeft();
        Predicate r2 = rightSplit.getRight();

        Predicate lower = conjunction(l1, r1);
        Predicate upper = conjunction(l2, r2);
        return new And(lower, upper);
    }

    public Predicate splitEquiJoin(Attrs leftAttrs, Attrs rightAttrs) {
        Predicate leftSplit = left.splitEquiJoin(leftAttrs, rightAttrs);
        Predicate l1 = ((And) leftSplit).getLeft();
        Predicate l2 = ((And) leftSplit).getRight();

        Predicate rightSplit = right.splitEquiJoin(leftAttrs, rightAttrs);
        Predicate r1 = ((And) rightSplit).getLeft();
        Predicate r2 = ((And) rightSplit).getRight();

        Predicate equiPred = conjunction(l1, r1);
        Predicate nonEquiPred = conjunction(l2, r2);
        return new And(equiPred, nonEquiPred);
    }

    public static Predicate conjunction(Predicate left, Predicate right) {
        True t = True.getTrue();
        if (left.equals(t))
            return right;
        if (right.equals(t))
            return left;
        return new And(left, right);
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        if (other == null || !(other instanceof And))
            return false;
        if (other.getClass() != And.class)
            return other.equals(this);
        And and = (And) other;
        return left.equals(and.getLeft()) && right.equals(and.getRight());

    }

    public int hashCode() {
        return left.hashCode() ^ right.hashCode();
    }

    Predicate negation() {
        return new Or(left.negation(), right.negation());
    }

    public float selectivity() {
        // Independence assumption
        return left.selectivity() * right.selectivity();
    }

    public void beginXML(StringBuffer sb) {
        sb.append("<and>");
    }

    public void childrenInXML(StringBuffer sb) {
        left.toXML(sb);
        right.toXML(sb);
    }

    public void endXML(StringBuffer sb) {
        sb.append("</and>");
    }

    public UpdateableEquiJoinPredicateList toEquiJoinPredicateList(
        Attrs leftAttrs,
        Attrs rightAttrs) {
        UpdateableEquiJoinPredicateList result =
            left.toEquiJoinPredicateList(leftAttrs, rightAttrs).updateableCopy();
        result.addAll(right.toEquiJoinPredicateList(leftAttrs, rightAttrs));
        return result;
    }
}
