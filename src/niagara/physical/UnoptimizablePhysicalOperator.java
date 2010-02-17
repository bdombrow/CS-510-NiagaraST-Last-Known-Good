package niagara.physical;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.query_engine.TupleSchema;
import niagara.utils.PEException;
import niagara.utils.SinkTupleStream;
import niagara.utils.SourceTupleStream;

public abstract class UnoptimizablePhysicalOperator extends PhysicalOperator {
	public UnoptimizablePhysicalOperator(SourceTupleStream[] sourceStreams,
			SinkTupleStream[] sinkStreams, boolean[] blockingSourceStreams,
			Integer responsiveness) {
		setBlockingSourceStreams(blockingSourceStreams);
		plugInStreams(sourceStreams, sinkStreams, null, responsiveness);
	}

	// The required zero-argument constructor
	public UnoptimizablePhysicalOperator() {
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
	 */
	public final PhysicalProperty findPhysProp(
			PhysicalProperty[] input_phys_props) {
		throw new PEException("Optimization is not supported for this operator");
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public final Cost findLocalCost(ICatalog catalog,
			LogicalProperty[] InputLogProp) {
		throw new PEException("Optimization is not supported for this operator");
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return System.identityHashCode(this);
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
	 */
	public void opInitFrom(LogicalOp op) {
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public final Op opCopy() {
		throw new PEException("Optimization is not supported for this operator");
	}

	public boolean equals(Object other) {
		return this == other;
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		;
	}

	public TupleSchema getTupleSchema() {
		throw new PEException("Optimization is not supported for this operator");
	}
}
