package niagara.query_engine;

import java.util.ArrayList;

import niagara.logical.IncrementalMax;
import niagara.utils.SinkTupleStream;
import niagara.utils.SourceTupleStream;
import niagara.utils.StreamTupleElement;
import niagara.xmlql_parser.op_tree.op;

import org.w3c.dom.Node;

public class PhysicalIncrementalMax extends PhysicalIncrementalGroup {
    private AtomicEvaluator ae;
    private ArrayList values;
    private Double emptyGroupValue;

    public PhysicalIncrementalMax(
        op logicalOperator,
        SourceTupleStream[] sourceStreams,
        SinkTupleStream[] sinkStreams,
        Integer responsiveness) {

        // Call the constructor of the super class
        super(logicalOperator, sourceStreams, sinkStreams, responsiveness);
    }

    public void opInitialize() {
        super.opInitialize();
        ae =
            new AtomicEvaluator(
                ((IncrementalMax) logicalGroupOperator).getMaxAttribute());
	emptyGroupValue = ((IncrementalMax) logicalGroupOperator).getEmptyGroupValue();
        values = new ArrayList();
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#processTuple(StreamTupleElement, Object)
     */
    public Object processTuple(
        StreamTupleElement tuple,
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

    public boolean outputOldValue() {
	return true;
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#constructOutput(Object)
     */
    public Node constructOutput(Object groupInfo) {
        return doc.createTextNode(String.valueOf(groupInfo));
    }
}
