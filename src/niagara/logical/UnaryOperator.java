package niagara.logical;

/** A unary operator */
public abstract class UnaryOperator extends LogicalOperator {

	/**
	 * @return the arity of the operator
	 */
	public final int getArity() {
		return 1;
	}
}
