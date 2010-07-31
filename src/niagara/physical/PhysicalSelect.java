package niagara.physical;

import java.util.ArrayList;

import niagara.logical.Select;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.physical.predicates.PredicateImpl;
import niagara.query_engine.TupleSchema;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.Log;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * Implementation of the Select operator.
 */

@SuppressWarnings("unchecked")
public class PhysicalSelect extends PhysicalOperator {
	// No blocking source streams
	private static final boolean[] blockingSourceStreams = { false };

	// The is the predicate to apply to the tuples
	private Predicate pred;
	private PredicateImpl predEval;

	// Propagate
	Boolean propagate = false;
	
	// logging test
	int tupleOut = 0;
	int tupleDrop = 0;

	public PhysicalSelect() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		pred = ((Select) logicalOperator).getPredicate();
		predEval = pred.getImplementation();
		logging = ((Select) logicalOperator).getLogging();
		if(logging) {
			log = new Log(this.getName());
		}
		propagate = ((Select)logicalOperator).getPropagate();
	}

	public Op opCopy() {
		PhysicalSelect p = new PhysicalSelect();
		p.pred = pred;
		p.predEval = predEval;
		p.log = log;
		p.logging = logging;
		p.propagate = propagate;
		return p;
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
	throws java.lang.InterruptedException, ShutdownException {
		// downstream control message is GET_PARTIAL
		// We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
		// REQ_BUF_FLUSH is handled inside SinkTupleStream
		// here (SHUTDOWN is handled with exceptions)

		if (ctrl == null)
			return;

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
			if(ctrl.size() > 2) {
			FeedbackPunctuation fp = (FeedbackPunctuation) ctrl.get(2);
			System.err.println("Received FP");
			
			System.out.println(ctrl.get(1).toString());
			System.out.println(fp.toString());
			
			if(propagate) {
				sendFeedbackPunctuation(fp, streamId);
			}
			} else {
				assert false : "Unexpected number of elements in MESSAGE.";
			}
			break;
		default:
			assert false : "KT unexpected control message from sink "
				+ ctrlFlag.flagName();
		}
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

	protected void processTuple(Tuple inputTuple, int streamId)
	throws ShutdownException, InterruptedException {
		// Evaluate the predicate on the desired attribute of the tuple

		if (predEval.evaluate(inputTuple, null)) {
			putTuple(inputTuple, 0);
			if(logging){
			tupleOut++;
			log.Update("TupleOut", String.valueOf(tupleOut));
			}
		}
		else {
			if(logging){
			tupleDrop++;
			log.Update("TupleDrop", String.valueOf(tupleDrop));
			}
		}	
	}		


	/**
	 * This function processes a punctuation element read from a source stream
	 * when the operator is non-blocking. This over-rides the corresponding
	 * function in the base class.
	 * 
	 * Punctuations can simply be sent to the next operator from Select
	 * 
	 * @param inputTuple
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */
	protected void processPunctuation(Punctuation inputTuple, int streamId)
	throws ShutdownException, InterruptedException {
		putTuple(inputTuple, 0);
	}

	public boolean isStateful() {
		return false;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		float InputCard = InputLogProp[0].getCardinality();
		Cost cost = new Cost(InputCard
				* catalog.getDouble("tuple_reading_cost"));
		cost.add(predEval.getCost(catalog).times(InputCard));
		return cost;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalSelect))
			return false;
		if (o.getClass() != PhysicalSelect.class)
			return o.equals(this);
		return pred.equals(((PhysicalSelect) o).pred);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return pred.hashCode() ^ logging.hashCode();
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
	 */
	public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
		return input_phys_props[0];
	}

	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		inputTupleSchemas = inputSchemas;
		outputTupleSchema = inputTupleSchemas[0];
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#opInitialize()
	 */
	protected void opInitialize() {
		predEval.resolveVariables(inputTupleSchemas[0], 0);
	}

	/**
	 * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
	 */
	public void dumpChildrenInXML(StringBuffer sb) {
		sb.append(">");
		pred.toXML(sb);
		sb.append("</").append(getName()).append(">");
	}
}
