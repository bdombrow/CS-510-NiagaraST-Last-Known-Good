package niagara.physical;

import niagara.logical.Expensive;
import niagara.logical.Instrument;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.FeedbackType;
import niagara.utils.Log;
import niagara.utils.OperatorDoneException;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * 
 * @author rfernand
 * @version 1.0
 * 
 *          The <code>PhysicalRename</code> operator is essentially the identity
 *          but with a fixed cost per tuple.
 * 
 */

public class PhysicalInstrument extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { false };

	private long interval;
	private Boolean propagate;
	private int outCount;

	public PhysicalInstrument() {
		setBlockingSourceStreams(blockingSourceStreams);
		interval = 0;
		propagate = false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		interval = ((Instrument) op).getInterval();
		logging = ((Instrument)op).getLogging();
		if(logging) {
			log = new Log(this.getName());
			outCount = 0;
		}
		propagate = ((Instrument)op).getPropagate();
	}

	@Override
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		float InputCard = InputLogProp[0].getCardinality();
		Cost cost = new Cost(InputCard
				* catalog.getDouble("tuple_reading_cost"));
		return cost;
	}

	@Override
	public boolean equals(Object other) {

		if (other.getClass() != this.getClass())
			return false;
		if (other == null)
			return false;
		if (((PhysicalInstrument) other).interval != this.interval)
			return false;
		if(((PhysicalInstrument)other).log != this.log)
			return false;
		if(((PhysicalInstrument)other).propagate != this.propagate)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return String.valueOf(interval).hashCode() ^ logging.hashCode() ^ propagate.hashCode();
	}

	@Override
	public Op opCopy() {
		PhysicalInstrument pr = new PhysicalInstrument();
		pr.interval = interval;
		pr.log = log;
		pr.logging = logging;
		pr.propagate = propagate;
		return pr;
	}

	protected void processTuple(Tuple tuple, int streamId)
			throws ShutdownException, InterruptedException,
			OperatorDoneException {

		putTuple(tuple, streamId);
		if(logging) {
			outCount++;
			log.Update("OutCount", String.valueOf(outCount));		
		}
		if(propagate){
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, outputTupleSchema, tuple);
			sendCtrlMsgToSource(ControlFlag.MESSAGE, "Your message", 0);
		}

	}

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {
		putTuple(tuple, streamId);
	}

}
