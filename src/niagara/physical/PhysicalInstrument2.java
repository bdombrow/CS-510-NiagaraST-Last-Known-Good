package niagara.physical;

import java.util.ArrayList;

import niagara.logical.Instrument2;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
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
 *          The <code>PhysicalInstrument2</code> operator is essentially the identity
 *          but sends punctuation every n tuples.
 * 
 */

public class PhysicalInstrument2 extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { false };

	private long interval;
	private Boolean propagate;
	private int sent = 0; // debug
	private int outCount;
	private Attribute tsAttr;
	private Attribute idAttr;
	private int tupleCount;

	public PhysicalInstrument2() {
		setBlockingSourceStreams(blockingSourceStreams);
		interval = 0;
		propagate = false;
		tsAttr = null;
		idAttr = null;
		tupleCount = 0;
		sent = 0;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		interval = ((Instrument2) op).getInterval();
		logging = ((Instrument2)op).getLogging();
		if(logging) {
			log = new Log(this.getName());
			outCount = 0;
		}
		propagate = ((Instrument2)op).getPropagate();
		tsAttr = ((Instrument2)op).getTsAttr();
		idAttr = ((Instrument2)op).getIdAttr();
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
		if (((PhysicalInstrument2) other).interval != this.interval)
			return false;
		if(((PhysicalInstrument2)other).log != this.log)
			return false;
		if(((PhysicalInstrument2)other).propagate != this.propagate)
			return false;
		if(((PhysicalInstrument2)other).tsAttr != this.tsAttr)
			return false;
		if(((PhysicalInstrument2)other).idAttr != this.idAttr)
			return false;
		if (((PhysicalInstrument2)other).tupleCount != this.tupleCount)
			return false;
		if (((PhysicalInstrument2)other).sent != this.sent)
			return false;
		

		return true;
	}

	@Override
	public int hashCode() {
		return String.valueOf(interval).hashCode() ^ logging.hashCode() ^ propagate.hashCode() ^ idAttr.hashCode() ^ tsAttr.hashCode() ^ tupleCount;
	}

	@Override
	public Op opCopy() {
		PhysicalInstrument2 pr = new PhysicalInstrument2();
		pr.interval = interval;
		pr.log = log;
		pr.logging = logging;
		pr.propagate = propagate;
		pr.tsAttr = tsAttr;
		pr.idAttr = idAttr;
		pr.tupleCount = tupleCount;
		pr.sent = sent;
		return pr;
	}

	protected void processTuple(Tuple tuple, int streamId)
			throws ShutdownException, InterruptedException,
			OperatorDoneException {
		putTuple(tuple, streamId);

		tupleCount++;
		
		if(logging) {
			outCount++;
			log.Update("OutCount", String.valueOf(outCount));		
		}
		if(propagate && ((tupleCount % interval) == 0)){

			// construct the FP element
			ArrayList<String> vars = new ArrayList<String>();
			ArrayList<FeedbackPunctuation.Comparator> comps = new ArrayList<FeedbackPunctuation.Comparator>();
			ArrayList<String> vals = new ArrayList<String>();
			
			// Add elements
			vars.add(tsAttr.getName());
			comps.add(FeedbackPunctuation.Comparator.LE);
			vals.add("634018212600000000");

			vars.add(idAttr.getName());
			comps.add(FeedbackPunctuation.Comparator.E);
			vals.add("1001");

			
			// Send elements
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, vars, comps, vals);
			sendFeedbackPunctuation(fp, 0);
			sent++;
		}

	}

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {
		putTuple(tuple, streamId);
	}

}
