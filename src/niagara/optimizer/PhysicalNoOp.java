package niagara.optimizer;

import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;

/** Physical NoOp is a do-nothing physical operator. It is only used
 * internally in the optimizer. */
public class PhysicalNoOp extends PhysicalOp implements Initializable {
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        return new Cost(0);
    }

    public Op opCopy() {
        return this;
    }

    public String getName() {
        return "NoOp";
    }

    public int getArity() {
        return 1;
    }

    public boolean equals(Object obj) {
        return this == obj;
    }

    public int hashCode() {
        return 0;
    }
    
    public final void initFrom(LogicalOp lop) {}
}
