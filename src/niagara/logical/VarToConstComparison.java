/* $Id$ */
package niagara.logical;
import java.util.ArrayList;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.PathToConstComparisonImpl;
import niagara.query_engine.VarToConstComparisonImpl;
import niagara.query_engine.PredicateImpl;
import niagara.xmlql_parser.syntax_tree.regExp;


public class VarToConstComparison extends Comparison {
    private Variable left;
    private Constant right;
    private regExp path;

    protected VarToConstComparison(int operator, Variable left, Constant right) {
        super(operator);
        this.left = left;
        this.right = right;
    }

    public PredicateImpl getImplementation() {
        // XXX vpapad: unnesting code not used yet
        if (path == null)
            return new VarToConstComparisonImpl(this);
        else
            return new PathToConstComparisonImpl(this);
    }

    public void getReferencedVariables(ArrayList al) {
        al.add(left);
    }
    
    public Atom getLeft() { return left; }
    
    public Atom getRight() { return right; }
    
    public regExp getPath() { return path; }
    
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
    public And split(Attrs variables) {
        if (variables.Contains(left))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}
