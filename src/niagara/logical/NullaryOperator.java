package niagara.logical;

/**
 * Nullary operators (operators with no inputs)
 */
abstract public class NullaryOperator extends LogicalOperator {
	public int getArity() {
		return 0;
	}
}
