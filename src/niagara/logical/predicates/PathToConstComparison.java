/* $Id: PathToConstComparison.java,v 1.1 2004/05/20 22:10:15 vpapad Exp $ */
package niagara.logical.predicates;
import java.util.ArrayList;
import niagara.logical.*;
import niagara.logical.path.RE;
import niagara.optimizer.colombia.Attrs;
import niagara.physical.predicates.PathToConstComparisonImpl;
import niagara.physical.predicates.PredicateImpl;
public class PathToConstComparison extends Comparison {
    private Variable var;
    private Constant right;
    private RE path;
    protected PathToConstComparison(int operator, Path left, Constant right) {
        super(operator);
        this.var = left.getVar();
        this.path = left.getPath();
        this.right = right;
    }
    public PredicateImpl getImplementation() {
        return new PathToConstComparisonImpl(this);
    }
    public void getReferencedVariables(ArrayList al) {
        al.add(var);
    }
    public Atom getLeft() {
        return var;
    }
    public Atom getRight() {
        return right;
    }
    public RE getPath() {
        return path;
    }
    public int hashCode() {
        return operator ^ var.hashCode() ^ path.hashCode() ^ right.hashCode();
    }
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PathToConstComparison))
            return false;
        if (o.getClass() != PathToConstComparison.class)
            return o.equals(this);
        PathToConstComparison v = (PathToConstComparison) o;
        return operator == v.operator && var.equals(v.var)
                && path.equals(v.path) && right.equals(v.right);
    }
    /**
     * @see niagara.logical.Predicate#split(Attrs)
     */
    public And split(Attrs variables) {
        if (variables.contains(var))
            return new And(this, True.getTrue());
        else
            return new And(True.getTrue(), this);
    }
}