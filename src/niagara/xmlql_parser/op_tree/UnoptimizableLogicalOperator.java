/* $Id$ */
package niagara.xmlql_parser.op_tree;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;

/** Trivial method definitions for operators that cannot be 
 * used with the optimizer */
public abstract class UnoptimizableLogicalOperator extends op {
    public final LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        throw new PEException("Optimization is not supported for this operator");
    }

    public final Op opCopy() {
        throw new PEException("Optimization is not supported for this operator");
    }
    
    public final int getArity() {
        return 1;
    }
    
    public int hashCode() {
        return System.identityHashCode(this);
    }
    
    public boolean equals(Object other) {
        return this == other;
    }
}
