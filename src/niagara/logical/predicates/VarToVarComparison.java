/* $Id: VarToVarComparison.java,v 1.1 2003/12/24 02:03:51 vpapad Exp $ */
package niagara.logical.predicates;
import java.util.ArrayList;

import niagara.logical.*;
import niagara.logical.UpdateableEquiJoinPredicateList;
import niagara.optimizer.colombia.Attrs;
import niagara.physical.predicates.PredicateImpl;
import niagara.physical.predicates.VarToVarComparisonImpl;
import niagara.utils.PEException;
import niagara.xmlql_parser.opType;

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
        if (variables.contains(left) && variables.contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }

    public Predicate splitEquiJoin(Attrs leftAttrs, Attrs rightAttrs) {
        if (operator == opType.EQ) {
            if (leftAttrs.contains(left) && rightAttrs.contains(right))
                return new And(this, True.getTrue());
            else if (leftAttrs.contains(right) && rightAttrs.contains(left))
                return new And(new VarToVarComparison(opType.EQ, right, left), True.getTrue());
        }
        
        return new And(True.getTrue(), this);
    }

    public UpdateableEquiJoinPredicateList toEquiJoinPredicateList(
        Attrs leftAttrs,
        Attrs rightAttrs) {
        UpdateableEquiJoinPredicateList result = new UpdateableEquiJoinPredicateList();
        if (leftAttrs.contains(left) && rightAttrs.contains(right))
            result.add(left, right);
        else
            throw new PEException("This comparison cannot be part of an equijoin predicate list");
        
        return result;
    }
}
