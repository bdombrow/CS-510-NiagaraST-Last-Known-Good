/* $Id: PhysicalJoin.java,v 1.2 2006/12/07 00:06:32 jinli Exp $ */
package niagara.physical;

import niagara.logical.Join;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.LogicalOp;
import niagara.query_engine.*;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;
import niagara.utils.PEException;

/** Common functionality for join implementations */
abstract public class PhysicalJoin extends PhysicalOperator {
    /** Are we projecting attributes away? */
    protected boolean projecting;
    /** Maps shared attribute positions between incoming and outgoing tuples */
    protected int[] leftAttributeMap;
    protected int[] rightAttributeMap;

    /** All predicates */
    protected Predicate joinPredicate;
    protected boolean extensionJoin[];

    public final void opInitFrom(LogicalOp logicalOperator) {
        Join join = (Join) logicalOperator;
        initJoin(join);
    }

    abstract protected void initJoin(Join join);

    protected void produceTuple(
        Tuple left,
        Tuple right)
        throws ShutdownException, InterruptedException {
        Tuple result;
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

    protected void initExtensionJoin(Join join) {
	if(extensionJoin == null)
	    extensionJoin = new boolean[2];
	
	switch(join.getExtensionJoin()) {
	case Join.LEFT:
	    extensionJoin[0] = true;
	    extensionJoin[1] = false;
	    break;
	case Join.RIGHT:
	    extensionJoin[0] = false;
	    extensionJoin[1] = true;
	    break;

	case Join.BOTH:
	    extensionJoin[0] = true;
	    extensionJoin[1] = true;
	    break;

	case Join.NONE:
	    extensionJoin[0] = false;
	    extensionJoin[1] = false;
	    break;
	default:
	    throw new PEException("Invalid extension join value");
	}
	return;
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
