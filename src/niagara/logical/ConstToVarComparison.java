/* ConstToVarComparison.java,v 1.1 2002/10/27 00:50:34 vpapad Exp */
package niagara.logical;
import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.ConstToVarComparisonImpl;
import niagara.query_engine.PredicateImpl;


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
        al.add(right.getName());
    }
    
    public Atom getLeft() {return left;}
    public Atom getRight() {return right;}
    
    /**
     * @see niagara.logical.Predicate#copy()
     */
    public Predicate copy() {
        return new ConstToVarComparison(getOperator(), left, right);
    }

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
    public Predicate split(Attrs variables) {
        if (variables.Contains(right))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}
