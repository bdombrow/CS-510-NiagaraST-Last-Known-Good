/* $Id: PhysicalIncrementalAverage.java,v 1.2 2002/10/24 23:15:15 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;

import niagara.logical.IncrementalAverage;
import niagara.utils.SinkTupleStream;
import niagara.utils.SourceTupleStream;
import niagara.utils.StreamTupleElement;
import niagara.xmlql_parser.op_tree.op;

import org.w3c.dom.Node;

public class PhysicalIncrementalAverage extends PhysicalIncrementalGroup {
    private AtomicEvaluator ae;
    private ArrayList values;

    class GroupStatistics {
	int count;
	double sum;
    }

    public void opInitialize() {
        super.opInitialize();
        ae =
            new AtomicEvaluator(
                ((IncrementalAverage) logicalGroupOperator).getAvgAttribute());
        values = new ArrayList();
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#processTuple(StreamTupleElement, Object)
     */
    public Object processTuple(
        StreamTupleElement tuple,
        Object previousGroupInfo) {
        GroupStatistics prevAverage = (GroupStatistics) previousGroupInfo;
	GroupStatistics newAverage;

        ae.getAtomicValues(tuple, values);
	try {
        double newValue = Double.parseDouble((String) values.get(0));
	values.clear();
	newAverage = new GroupStatistics();

	// New group
	if(prevAverage == null) {
	    newAverage.count = 1;
	    newAverage.sum = newValue;
	    return newAverage;
	}
	// We already have statistics for the group
	newAverage.count = prevAverage.count + 1;
	newAverage.sum = prevAverage.sum + newValue;
	double newAvg = newAverage.sum / newAverage.count;
	double prevAvg = prevAverage.sum / prevAverage.count;
	// We're messing with floating point arithmetic here,
	// this test may fail...
	if (newAvg != prevAvg) 
	    return newAverage;
	else
	    // No change in group
	    return prevAverage;
	} catch (NumberFormatException nfe) {
	    throw new RuntimeException("XXX vpapad what do we do here?!");
	}
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#emptyGroupValue()
     */
    public Object emptyGroupValue() {
        return null;
    }

    /**
     * @see niagara.query_engine.PhysicalIncrementalGroup#constructOutput(Object)
     */
    public Node constructOutput(Object groupInfo) {
	GroupStatistics gs = (GroupStatistics) groupInfo;
	double avg = gs.sum / gs.count;
        return doc.createTextNode(String.valueOf(avg));
    }
}
