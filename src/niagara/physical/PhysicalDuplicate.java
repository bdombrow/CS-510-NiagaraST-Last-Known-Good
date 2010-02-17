package niagara.physical;

import niagara.logical.Duplicate;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * This is the <code>PhysicalDuplicateOperator</code> that extends the basic
 * PhysicalOperator. The Duplicate operator duplicates the contents of its input
 * stream to all its output streams.
 * 
 * @version 1.0
 * 
 */

public class PhysicalDuplicate extends PhysicalOperator {
	private int numOutputStreams;
	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false };

	public PhysicalDuplicate() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		numOutputStreams = ((Duplicate) logicalOperator).getNumberOfOutputs();
	}

	/**
	 * This function processes a tuple element read from a source stream when
	 * the operator is non-blocking. This over-rides the corresponding function
	 * in the base class.
	 * 
	 * @param tupleElement
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */

	protected void processTuple(Tuple tupleElement, int streamId)
			throws ShutdownException, InterruptedException {
		// Copy the input tuple to all the sink streams
		for (int dest = 0; dest < numSinkStreams; ++dest) {
			putTuple(tupleElement, dest);
		}
	}

	protected void processPunctuation(Punctuation tupleElement, int streamId)
			throws ShutdownException, InterruptedException {
		// Copy the input tuple to all the sink streams
		for (int dest = 0; dest < numSinkStreams; ++dest) {
			putTuple(tupleElement, dest);
		}
	}

	public boolean isStateful() {
		return false;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalDuplicate))
			return false;
		if (o.getClass() != PhysicalDuplicate.class)
			return o.equals(this);
		return numOutputStreams == ((PhysicalDuplicate) o).numOutputStreams;
	}

	public int hashCode() {
		return numOutputStreams;
	}

	public Op opCopy() {
		PhysicalDuplicate op = new PhysicalDuplicate();
		op.numOutputStreams = numOutputStreams;
		return op;
	}

	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		inputTupleSchemas = inputSchemas;
		outputTupleSchema = inputTupleSchemas[0];
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#findLocalCost(ICatalog,
	 *      LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		float inputCard = inputLogProp[0].getCardinality();
		float outputCard = logProp.getCardinality();

		double cost = inputCard * catalog.getDouble("tuple_reading_cost");
		cost += outputCard * catalog.getDouble("tuple_construction_cost");
		return new Cost(cost);
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#findPhysProp(PhysicalProperty[])
	 */
	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
		return input_phys_props[0];
	}

	/**
	 * @see niagara.optimizer.colombia.Op#getNumberOfOutputs()
	 */
	public int getNumberOfOutputs() {
		return numOutputStreams;
	}
}
