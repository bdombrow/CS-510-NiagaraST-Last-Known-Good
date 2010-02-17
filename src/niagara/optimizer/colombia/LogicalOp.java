package niagara.optimizer.colombia;

/***
 * Logical operators
 */
@SuppressWarnings("unchecked")
public abstract class LogicalOp extends Op {
	/**
	 * @return <code>true</code> if <code>this</code> and <code>other</code> are
	 *         the same operator, disregarding arguments. OpMatch is used in
	 *         preconditions for applying rules. This should be moved to the Op
	 *         class if we ever apply rules to other than logical operators.
	 */
	public boolean opMatch(Class other) {
		return getClass() == other;
	}

	/**
	 * Determine the logical properties of this operator's output, given the
	 * logical properties of its inputs
	 */
	public abstract LogicalProperty findLogProp(ICatalog catalog,
			LogicalProperty[] input);

	public boolean isLogical() {
		return true;
	}
}
