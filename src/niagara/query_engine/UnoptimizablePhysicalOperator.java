/* $Id: UnoptimizablePhysicalOperator.java,v 1.5 2002/10/31 03:54:38 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;

import niagara.optimizer.colombia.*;
import niagara.utils.PEException;
import niagara.utils.SinkTupleStream;
import niagara.utils.SourceTupleStream;
import niagara.xmlql_parser.op_tree.op;

public abstract class UnoptimizablePhysicalOperator extends PhysicalOperator {
    public UnoptimizablePhysicalOperator(
        SourceTupleStream[] sourceStreams,
        SinkTupleStream[] sinkStreams,
        boolean[] blockingSourceStreams,
        Integer responsiveness) {
        setBlockingSourceStreams(blockingSourceStreams);
        plugInStreams(sourceStreams, sinkStreams, responsiveness);
    }

    // The required zero-argument constructor
    public UnoptimizablePhysicalOperator() {}

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public final PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
        throw new PEException("Optimization is not supported for this operator");
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public final Cost findLocalCost(
        ICatalog catalog,
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
    public void initFrom(LogicalOp op) {
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public final Op copy() {
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
