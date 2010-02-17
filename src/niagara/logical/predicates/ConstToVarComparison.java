package niagara.logical.predicates;

import java.util.ArrayList;

import niagara.logical.Variable;
import niagara.optimizer.colombia.Attrs;
import niagara.physical.predicates.ConstToVarComparisonImpl;
import niagara.physical.predicates.PredicateImpl;


@SuppressWarnings("unchecked")
public class ConstToVarComparison extends Comparison {
    private Constant left;
    private Variable right;

    protected ConstToVarComparison(int operator, Constant left, Variable right) {
        super(operator);
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        return new ConstToVarComparisonImpl(this);
    }

    public void getReferencedVariables(ArrayList al) {
        al.add(right);
    }
    
    public Atom getLeft() {return left;}
    public Atom getRight() {return right;}
    
    public int hashCode() { 
        return operator ^ left.hashCode() ^ right.hashCode();
    }    

    public boolean equals(Object o) {
        if (o == null || !(o instanceof ConstToVarComparison))
            return false;
        if (o.getClass() != ConstToVarComparison.class)
            return o.equals(this);
        ConstToVarComparison v = (ConstToVarComparison) o;
        return operator == v.operator && left.equals(v.left) && right.equals(v.right);
    }
    
    /**
     * @see niagara.logical.Predicate#split(Attrs)
     */
    public And split(Attrs variables) {
        if (variables.contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}
