/*
  $Id: PhysicalPrefix.java,v 1.2 2003/02/26 06:35:12 tufte Exp $
*/

package niagara.query_engine;

import niagara.utils.*;

import niagara.logical.Prefix;
import niagara.optimizer.colombia.*;

/** The PhysicalPrefix operator outputs up to a given number of
 * tuples from its input stream before shutting down */
public class PhysicalPrefix extends PhysicalOperator {
    // No blocking source streams
    private static final boolean[] blockingSourceStreams = { false };

    /** Pass through up to <code>length</code> tuples before shutting down */
    private int length;

    private int cnt;

    /** Number of tuples that passed through so far */
    private int tuplesPassed;

    public PhysicalPrefix() {
        setBlockingSourceStreams(blockingSourceStreams);
        tuplesPassed = 0;
    }

    public void initFrom(LogicalOp logicalOperator) {
        Prefix p = (Prefix) logicalOperator;
        this.length = p.getLength();
    }

    public Op copy() {
        PhysicalPrefix p = new PhysicalPrefix();
        p.length = length;
        return p;
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void nonblockingProcessSourceTupleElement(
        StreamTupleElement inputTuple,
        int streamId)
        throws ShutdownException, InterruptedException {
        if (!inputTuple.isPartial() && ++tuplesPassed > length) {
            throw new ShutdownException("Query shut down by Prefix Operator");
	}
        else
            putTuple(inputTuple, 0);
        cnt++;
        System.err.println("XXX vpapad: prefix tuple sent " + cnt);
    }

    public boolean isStateful() {
        return false;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
        float inputCard = inputLogProp[0].getCardinality();
        return 
            new Cost(inputCard * catalog.getDouble("tuple_reading_cost"));
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalPrefix))
            return false;
        if (o.getClass() != PhysicalPrefix.class)
            return o.equals(this);
        return length == ((PhysicalPrefix) o).length;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return length;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
        return input_phys_props[0];
    }

    /* 
     * @see niagara.utils.SerializableToXML#dumpAttributesInXML(java.lang.StringBuffer)
     */
    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" length='").append(length).append("'");
    }
}
