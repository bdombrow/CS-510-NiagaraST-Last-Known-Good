/* $Id$ */
package niagara.optimizer;

import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;

/**
 * AnyLogicalOp matches any logical operator
 */
public class AnyLogicalOp extends LogicalOp {

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        throw new PEException("AnyLogicalOp has indeterminate logical properties");
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        return new AnyLogicalOp();
    }

    /**
     * @see niagara.optimizer.colombia.Op#getName()
     */
    public String getName() {
        return "AnyLogicalOp";
    }

    /** */
    public int getArity() {
        throw new PEException("AnyLogicalOp has different arities at different times");
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        return (obj != null && obj instanceof AnyLogicalOp);
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return 0;
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#opMatch(LogicalOp)
     */
    public boolean opMatch(LogicalOp other) {
        return true;
    }
    /**
     * @see niagara.optimizer.colombia.Op#matches(Op)
     */
    public boolean matches(Op other) {
        return true;
    }

}
