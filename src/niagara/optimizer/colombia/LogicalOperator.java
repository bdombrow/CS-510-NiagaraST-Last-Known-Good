package niagara.optimizer.colombia;

/** Logical operators */
public abstract class LogicalOperator extends OP {
    /**
     * @returns <code>true</code> if <code>this</code> and
     * <code>other</code> are the same operator, disregarding
     * arguments. OpMatch is used in preconditions for applying rules.
     * This should be moved to the OP class if we ever apply rules to
     * other than logical operators.
     */
    public boolean opMatch(LogicalOperator other) {
        // XXX vpapad: changed this to use Java's getClass()
	// may be too expensive
	return (getClass() == other.getClass());  
    }
    
    public boolean is_logical() {return true;}
}
