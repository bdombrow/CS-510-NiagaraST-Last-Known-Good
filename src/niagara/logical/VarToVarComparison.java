/* $Id: VarToVarComparison.java,v 1.3 2002/12/03 23:13:33 vpapad Exp $ */
package niagara.logical;
import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.VarToVarComparisonImpl;
import niagara.query_engine.PredicateImpl;


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

    public Atom getLeft() { return left; }
    public Atom getRight() { return right; }

    public Predicate copy() {
        return new VarToVarComparison(getOperator(), left, right);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof VarToVarComparison))
            return false;
        if (o.getClass() != VarToVarComparison.class)
            return o.equals(this);
        VarToVarComparison v = (VarToVarComparison) o;
        return operator == v.operator && left.equals(v.left) && right.equals(v.right);
    }
    
    public int hashCode() {
        return operator ^ left.hashCode() ^ right.hashCode();
    }

    public Predicate split(Attrs variables) {
        if (variables.Contains(left) && variables.Contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
    
    public Predicate splitEquiJoin(Attrs leftAttrs, Attrs rightAttrs) {
        if (leftAttrs.Contains(left) && rightAttrs.Contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}
