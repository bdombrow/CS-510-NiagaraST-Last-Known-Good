package niagara.physical;

import niagara.logical.Expensive;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
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

public class PhysicalExpensive extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { false };

	private long cost;
	private int outCount;

	public PhysicalExpensive() {
		setBlockingSourceStreams(blockingSourceStreams);
		cost = 0;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		cost = ((Expensive) op).getCost();
		logging = ((Expensive)op).getLogging();
		if(logging) {
			log = new Log(this.getName());
			outCount = 0;
		}
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
		if (((PhysicalExpensive) other).cost != this.cost)
			return false;
		if(((PhysicalExpensive)other).log != this.log)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return String.valueOf(cost).hashCode() ^ logging.hashCode();
	}

	@Override
	public Op opCopy() {
		PhysicalExpensive pr = new PhysicalExpensive();
		pr.cost = cost;
		pr.log = log.Copy();
		pr.logging = logging;
		return pr;
	}

	protected void processTuple(Tuple tuple, int streamId)
			throws ShutdownException, InterruptedException,
			OperatorDoneException {

		for (int i = 1; i <= cost; i++) {
			// something expensive
		}
		putTuple(tuple, streamId);
		if(logging) {
			outCount++;
			log.Update("OutCount", String.valueOf(outCount));		
		}

	}

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {
		putTuple(tuple, streamId);
	}

}
