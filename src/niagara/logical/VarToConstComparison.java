/* $Id$ */
package niagara.logical;
import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.VarToConstComparisonImpl;
import niagara.query_engine.VarToVarComparisonImpl;
import niagara.query_engine.PredicateImpl;


public class VarToConstComparison extends Comparison {
    private Variable left;
    private Constant right;

    protected VarToConstComparison(int operator, Variable left, Constant right) {
        super(operator);
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        return new VarToConstComparisonImpl(this);
    }

    public void getReferencedVariables(ArrayList al) {
        al.add(left.getName());
    }
    
    public Atom getLeft() { return left; }
    
    public Atom getRight() { return right; }
    
    public int hashCode() { return operator ^ left.hashCode() ^ right.hashCode(); }
    
    public boolean equals(Object o) {
        if (o == null || !(o instanceof VarToConstComparison))
            return false;
        if (o.getClass() != VarToConstComparison.class)
            return o.equals(this);
        VarToConstComparison v = (VarToConstComparison) o;
        return operator == v.operator && left.equals(v.left) && right.equals(v.right);
    }
    
    /**
     * @see niagara.logical.Predicate#copy()
     */
    public Predicate copy() {
        return new VarToConstComparison(getOperator(), left, right);
    }

    /**
     * @see niagara.logical.Predicate#split(Attrs)
     */
    public Predicate split(Attrs variables) {
        if (variables.Contains(left))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}
