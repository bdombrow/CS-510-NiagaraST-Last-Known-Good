/* $Id: PhysicalIncrementalMax.java,v 1.1 2003/12/24 01:49:03 vpapad Exp $ */
package niagara.physical;

import java.util.ArrayList;

import niagara.logical.IncrementalMax;
import niagara.optimizer.colombia.*;
import niagara.utils.*;
import org.w3c.dom.Node;

public class PhysicalIncrementalMax extends PhysicalIncrementalGroup {
    private Attribute maxAttribute;
    private AtomicEvaluator ae;
    private ArrayList values;
    private Double emptyGroupValue;

    public void opInitFrom(LogicalOp logicalOperator) {
        super.opInitFrom(logicalOperator);
        // Get the max attribute 
        maxAttribute = ((IncrementalMax) logicalOperator).getMaxAttribute();
        emptyGroupValue = ((IncrementalMax) logicalOperator).getEmptyGroupValue();
    }

    public void opInitialize() {
        super.opInitialize();
        ae = new AtomicEvaluator(maxAttribute.getName());
        ae.resolveVariables(inputTupleSchemas[0], 0);
        values = new ArrayList();
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#processTuple(Tuple, Object)
     */
    public Object processTuple(
        Tuple tuple,
        Object previousGroupInfo) {
        Double prevMax = (Double) previousGroupInfo;
        ae.getAtomicValues(tuple, values);
	try {
        Double newValue = Double.valueOf((String) values.get(0));
	values.clear();
        Double newMax = new Double(
            Math.max(prevMax.doubleValue(), newValue.doubleValue()));
	if (newMax.equals(prevMax)) {
	    // No change in group
	    return prevMax;
	} else
	    return newMax;
	} catch (NumberFormatException nfe) {
	    throw new RuntimeException("XXX vpapad what do we do here?!");
	}
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#emptyGroupValue()
     */
    public Object emptyGroupValue() {
        return emptyGroupValue;
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#constructOutput(Object)
     */
    public Node constructOutput(Object groupInfo) {
        return doc.createTextNode(String.valueOf(groupInfo));
    }

    public Op opCopy() {
        PhysicalIncrementalMax op = new PhysicalIncrementalMax();
        if (logicalGroupOperator != null)
            op.initFrom(logicalGroupOperator);
        return op;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalIncrementalMax))
            return false;
        if (o.getClass() != PhysicalIncrementalMax.class)
            return o.equals(this);
        return logicalGroupOperator.equals(
            ((PhysicalIncrementalMax) o).logicalGroupOperator);
    }

    public int hashCode() {
        return logicalGroupOperator.hashCode();
    }
}
