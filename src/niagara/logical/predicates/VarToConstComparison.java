package niagara.logical.predicates;

import java.util.ArrayList;

import niagara.logical.Variable;
import niagara.logical.path.RE;
import niagara.optimizer.colombia.Attrs;
import niagara.physical.predicates.PathToConstComparisonImpl;
import niagara.physical.predicates.PredicateImpl;
import niagara.physical.predicates.VarToConstComparisonImpl;

@SuppressWarnings("unchecked")
public class VarToConstComparison extends Comparison {
	private Variable left;
	private Constant right;
	private RE path;

	protected VarToConstComparison(int operator, Variable left, Constant right) {
		super(operator);
		this.left = left;
		this.right = right;
	}

	public PredicateImpl getImplementation() {
		if (path == null)
			return new VarToConstComparisonImpl(this);
		else
			return new PathToConstComparisonImpl(this);
	}

	public void getReferencedVariables(ArrayList al) {
		al.add(left);
	}

	public Atom getLeft() {
		return left;
	}

	public Atom getRight() {
		return right;
	}

	public RE getPath() {
		return path;
	}

	public int hashCode() {
		return operator ^ left.hashCode() ^ right.hashCode();
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof VarToConstComparison))
			return false;
		if (o.getClass() != VarToConstComparison.class)
			return o.equals(this);
		VarToConstComparison v = (VarToConstComparison) o;
		return operator == v.operator && left.equals(v.left)
				&& right.equals(v.right);
	}

	/**
	 * @see niagara.logical.Predicate#split(Attrs)
	 */
	public And split(Attrs variables) {
		if (variables.contains(left))
			return new And(this, True.getTrue());
		else
			return new And(True.getTrue(), this);
	}
}
