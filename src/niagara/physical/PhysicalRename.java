package niagara.physical;

import java.util.Vector;

import niagara.logical.Rename;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.OperatorDoneException;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * 
 * @author rfernand
 * @version 1.0
 * 
 * The <code>PhysicalRename</code> operator is essentially the identity; attribute renaming occurs in its logical counterpart.
 *
 */

public class PhysicalRename extends PhysicalOperator {

	private static final boolean[] blockingSourceStreams = { false };

	private Vector<String> newNames;
	private Vector<Attribute> attributesToRename;

	public PhysicalRename() {
		setBlockingSourceStreams(blockingSourceStreams);
		attributesToRename = new Vector<Attribute>();
		newNames = new Vector<String>();
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	protected void opInitFrom(LogicalOp op) {
		attributesToRename = ((Rename)op).getAttributesToRename();
		newNames = ((Rename)op).getNewNames();
	}

	@Override
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] InputLogProp) {
		float InputCard = InputLogProp[0].getCardinality();
		Cost cost = new Cost(InputCard * catalog.getDouble("tuple_reading_cost"));
		return cost;
	}

	@Override
	public boolean equals(Object other) {

		if(other.getClass() != this.getClass())
			return false;
		if(other == null)
			return false;
		if(((PhysicalRename)other).attributesToRename != this.attributesToRename)
			return false;
		if(((PhysicalRename)other).newNames != this.newNames)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return attributesToRename.hashCode() ^ newNames.hashCode();
	}

	@Override
	public Op opCopy() {
		PhysicalRename pr = new PhysicalRename();
		pr.attributesToRename = attributesToRename;
		pr.newNames = newNames;
		return pr;
	}

	protected void processTuple(Tuple tuple, int streamId)
	throws ShutdownException, InterruptedException,
	OperatorDoneException {
		putTuple(tuple, streamId);
	}

	protected void processPunctuation(Punctuation tuple, int streamId)
	throws ShutdownException, InterruptedException {
		putTuple(tuple, streamId);
	}

}
