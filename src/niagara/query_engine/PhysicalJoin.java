/* $Id: PhysicalJoin.java,v 1.1 2002/12/10 01:17:45 vpapad Exp $ */
package niagara.query_engine;

import niagara.logical.Predicate;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.ShutdownException;
import niagara.utils.StreamTupleElement;
import niagara.xmlql_parser.op_tree.joinOp;

/** Common functionality for join implementations */
abstract public class PhysicalJoin extends PhysicalOperator {
    /** Are we projecting attributes away? */
    private boolean projecting;
    /** Maps shared attribute positions between incoming and outgoing tuples */
    private int[] leftAttributeMap;
    private int[] rightAttributeMap;

    /** All predicates */
    protected Predicate joinPredicate;

    public final void initFrom(LogicalOp logicalOperator) {
        joinOp join = (joinOp) logicalOperator;
        initJoin(join);
    }

    abstract protected void initJoin(joinOp join);

    protected void produceTuple(
        StreamTupleElement left,
        StreamTupleElement right)
        throws ShutdownException, InterruptedException {
        StreamTupleElement result;
        if (projecting) {
            result = left.copy(outputTupleSchema.getLength(), leftAttributeMap);
            right.copyInto(result, leftAttributeMap.length, rightAttributeMap);
        } else {
            result = left.copy(outputTupleSchema.getLength());
            result.appendTuple(right);
        }

        // Add the result to the output
        putTuple(result, 0);
    }

    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(getClass())))
            return false;
        PhysicalJoin join = (PhysicalJoin) o;
        return joinPredicate.equals(join.joinPredicate)
            && equalsNullsAllowed(getLogProp(), join.getLogProp());
    }

    public int hashCode() {
        return joinPredicate.hashCode() ^ hashCodeNullsAllowed(getLogProp());
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;

        // We can't depend on our logical property's attribute
        // order, since commutes etc. may have changed it
        outputTupleSchema = inputSchemas[0].copy();
        outputTupleSchema.addMappings(inputSchemas[1].getAttrs());
        
        projecting = inputSchemas[0].getLength() + inputSchemas[1].getLength() > outputTupleSchema.getLength();
        if (projecting) {
            leftAttributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
            rightAttributeMap = inputSchemas[1].mapPositions(outputTupleSchema);
        }
    }
    
    
    /**
     * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
     */
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        joinPredicate.toXML(sb);
        sb.append("</").append(getName()).append(">");
    }
}