/* $Id: LogicalOp.java,v 1.6 2002/10/31 04:23:00 vpapad Exp $ */
package niagara.optimizer.colombia;

import java.util.ArrayList;

/** Logical operators */
public abstract class LogicalOp extends Op {
    /**
     * @returns <code>true</code> if <code>this</code> and
     * <code>other</code> are the same operator, disregarding
     * arguments. OpMatch is used in preconditions for applying rules.
     * This should be moved to the Op class if we ever apply rules to
     * other than logical operators.
     */
    public boolean opMatch(LogicalOp other) {
        // XXX vpapad: changed this to use Java's getClass()
        // may be too expensive
        return (getClass() == other.getClass());
    }

    /**
     * Determine the logical properties of this operator's output,
     * given the logical properties of its inputs
     */
    public abstract LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input);
        
    public boolean is_logical() {
        return true;
    }
}
