package niagara.logical;

/**
 * This class is used to represent binary operators.
 * 
 */

abstract public class BinaryOperator extends LogicalOperator {
	/**
	 * @return the arity of the operator (2 in this case)
	 */
	public int getArity() {
		return 2;
	}
}
