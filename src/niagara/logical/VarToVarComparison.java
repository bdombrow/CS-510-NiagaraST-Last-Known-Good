/* $Id: VarToVarComparison.java,v 1.4 2002/12/10 01:21:22 vpapad Exp $ */
package niagara.logical;
import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.VarToVarComparisonImpl;
import niagara.query_engine.PredicateImpl;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.opType;

public class VarToVarComparison extends Comparison {
    private Variable left;
    private Variable right;

    protected VarToVarComparison(int operator, Variable left, Variable right) {
        super(operator);
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        return new VarToVarComparisonImpl(this);
    }

    public void getReferencedVariables(ArrayList al) {
        al.add(left);
        al.add(right);
    }

    public Atom getLeft() {
        return left;
    }
    public Atom getRight() {
        return right;
    }

    public Predicate copy() {
        return new VarToVarComparison(getOperator(), left, right);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof VarToVarComparison))
            return false;
        if (o.getClass() != VarToVarComparison.class)
            return o.equals(this);
        VarToVarComparison v = (VarToVarComparison) o;
        return operator == v.operator
            && left.equals(v.left)
            && right.equals(v.right);
    }

    public int hashCode() {
        return operator ^ left.hashCode() ^ right.hashCode();
    }

    public And split(Attrs variables) {
        if (variables.Contains(left) && variables.Contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }

    public Predicate splitEquiJoin(Attrs leftAttrs, Attrs rightAttrs) {
        if (operator == opType.EQ) {
            if (leftAttrs.Contains(left) && rightAttrs.Contains(right))
                return new And(this, True.getTrue());
            else if (leftAttrs.Contains(right) && rightAttrs.Contains(left))
                return new And(new VarToVarComparison(opType.EQ, right, left), True.getTrue());
        }
        
        return new And(True.getTrue(), this);
    }

    public EquiJoinPredicateList toEquiJoinPredicateList(
        Attrs leftAttrs,
        Attrs rightAttrs) {
        EquiJoinPredicateList result = new EquiJoinPredicateList();
        if (leftAttrs.Contains(left) && rightAttrs.Contains(right))
            result.add(left, right);
        else
            throw new PEException("This comparison cannot be part of an equijoin predicate list");
        
        return result;
    }
}
