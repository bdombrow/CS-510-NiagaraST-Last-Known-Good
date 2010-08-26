package niagara.physical;

import java.util.ArrayList;

import niagara.logical.Instrument3;
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
 *          The <code>PhysicalInstrument3</code> operator is essentially the identity
 *          but sends punctuation every n tuples.
 * 
 */

public class PhysicalInstrument3 extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { false };

	private long interval;
	private Boolean propagate;
	private int sent = 0; // debug
	private int outCount;
	private Attribute tsAttr;
	private Attribute idAttr;
	private int tupleCount;
	boolean sentOne = false;
	boolean pass_punct = true;
	
	private String fattrs;
	private String comparators;
	private String values;
	

	public PhysicalInstrument3() {
		setBlockingSourceStreams(blockingSourceStreams);
		interval = 0;
		propagate = false;
		tsAttr = null;
		idAttr = null;
		tupleCount = 0;
		sent = 0;
		
		fattrs = "";
		comparators = "";
		values = "";
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		interval = ((Instrument3) op).getInterval();
		logging = ((Instrument3)op).getLogging();
		if(logging) {
			log = new Log(this.getName());
			outCount = 0;
		}
		
		propagate = ((Instrument3)op).getPropagate();
		pass_punct = ((Instrument3)op).getPrintPunct(); 
		fattrs = ((Instrument3)op).getFAttrs();
		comparators = ((Instrument3)op).getComparators();
		values = ((Instrument3)op).getValues();
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
		if (((PhysicalInstrument3) other).interval != this.interval)
			return false;
		if(((PhysicalInstrument3)other).log != this.log)
			return false;
		if(((PhysicalInstrument3)other).propagate != this.propagate)
			return false;
		if(((PhysicalInstrument3)other).pass_punct != this.pass_punct)
			return false;
		if(((PhysicalInstrument3)other).fattrs != this.fattrs)
			return false;
		if(((PhysicalInstrument3)other).comparators != this.comparators)
			return false;
		if(((PhysicalInstrument3)other).values != this.values)
			return false;
		if (((PhysicalInstrument3)other).tupleCount != this.tupleCount)
			return false;
		if (((PhysicalInstrument3)other).sent != this.sent)
			return false;
		

		return true;
	}

	@Override
	public int hashCode() {
		return String.valueOf(interval).hashCode() ^ logging.hashCode() ^ propagate.hashCode() ^ tupleCount;
	}

	@Override
	public Op opCopy() {
		PhysicalInstrument3 pr = new PhysicalInstrument3();
		pr.interval = interval;
		pr.log = log;
		pr.logging = logging;
		pr.propagate = propagate;
		pr.pass_punct = pass_punct;
		pr.comparators = comparators;
		pr.fattrs = fattrs;
		pr.values = values; 
		pr.tupleCount = tupleCount;
		pr.sent = sent;
		return pr;
	}

	protected void processTuple(Tuple tuple, int streamId)
			throws ShutdownException, InterruptedException,
			OperatorDoneException {
		putTuple(tuple, streamId);

		tupleCount++;
		//System.out.println(this.getName() + tupleCount);
		
		if(logging) {
			outCount++;
			log.Update("OutCount", String.valueOf(outCount));		
		}
		if((!sentOne) && propagate && ((tupleCount % interval) == 0)){
			sentOne = true;
			
			// construct the FP element
			String[] varsS = fattrs.split(" ");
			String[] compsS = comparators.split(" ");
			String[] valsS = values.split(" ");
			
			ArrayList<String> vars = new ArrayList<String>();
			ArrayList<FeedbackPunctuation.Comparator> comps = new ArrayList<FeedbackPunctuation.Comparator>();
			ArrayList<String> vals = new ArrayList<String>();
			
			for(int i = 0; i < varsS.length; i++) {
				vars.add(varsS[i]);
				vals.add(valsS[i]);
				
				if(compsS[i].equals("LT"))
					comps.add(FeedbackPunctuation.Comparator.LT);
				else if(compsS[i].equals("LE"))
					comps.add(FeedbackPunctuation.Comparator.LE);
				else if(compsS[i].equals("E"))
					comps.add(FeedbackPunctuation.Comparator.E);
				else if(compsS[i].equals("GE"))
					comps.add(FeedbackPunctuation.Comparator.GE);
				else
					comps.add(FeedbackPunctuation.Comparator.GT);
			}
			
			vars.trimToSize();
			vals.trimToSize();
			comps.trimToSize();
			
			// Send elements
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, vars, comps, vals);
			sendFeedbackPunctuation(fp, 0);
			System.out.println(this.getName() + fp.toString());
			sent++;
		}

	}

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {
		if(pass_punct)
			putTuple(tuple, streamId);
		}

}


