package niagara.physical;

import java.util.Iterator;
import java.util.Vector;

import niagara.logical.Present;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.DuplicateHashtable;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

import org.w3c.dom.Document;

/**
 * This is the <code>PhysicalHashJoinOperator</code> that extends the basic
 * PhysicalOperator with the implementation of the Hash Join operator.
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public class PhysicalPresent extends PhysicalOperator {
	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false, false };

	private Hasher[] hashers;

	private Punctuation lastPunct;

	// The array of hash tables of partial tuple elements that are read from the
	// source streams. The index of the array corresponds to the index of the
	// stream from which the tuples were read.
	DuplicateHashtable[] partialSourceTuples;

	// The array of hash tables of final tuple elements that are read from the
	// source streams. The index of the array corresponds to the index of the
	// stream from which the tuples were read.
	DuplicateHashtable[] finalSourceTuples;

	SimpleAtomicEvaluator[] ts = null;
	int[] granularity = null;
	Attrs[] punctAttrs = null;
	Document doc;

	public PhysicalPresent() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public final void opInitFrom(LogicalOp logicalOperator) {
		punctAttrs = ((Present) logicalOperator).getPunctAttrs();
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

	protected void processTuple(Tuple tupleElement, int streamId)
			throws ShutdownException, InterruptedException {

		if (streamId != 0) {
			// System.err.println("tuple from stream "+streamId+", which should only contain punctuations");
			return;
		}

		// Get the hash code corresponding to the tuple element
		String hashKey = hashers[streamId].hashKey(tupleElement);

		// ignore null attributes...
		if (hashKey == null)
			return;

		Tuple left, right;

		left = tupleElement;
		right = null;

		// timestamp value of the tuple
		long timestamp = Long.valueOf(ts[streamId].getAtomicValue(left, right)
				.trim());

		// Add the tuple element to the appropriate hash table,
		// but only if we haven't yet seen a punctuation from the
		// other input that matches it; otherwise, output the tuple
		boolean fMatch = false;

		if (lastPunct != null) {
			left = null;
			right = lastPunct;
			int otherStreamId = 1 - streamId;

			String punctVal = ts[otherStreamId].getAtomicValue(left, right)
					.trim();

			if (punctVal.startsWith("("))
				punctVal = punctVal.substring(2, punctVal.length() - 1);

			// timestamp value of the last punctuation from the other stream
			long punctTS = Long.valueOf(punctVal);
			fMatch = (punctTS >= timestamp);
		}

		if (!fMatch) {
			if (tupleElement.isPartial()) {
				partialSourceTuples[streamId].put(hashKey, tupleElement);
			} else {
				finalSourceTuples[streamId].put(hashKey, tupleElement);
			}
		} else {
			putTuple(tupleElement, 0);
		}
	}

	/**
	 * This function removes the effects of the partial results in a given
	 * source stream. This function over-rides the corresponding function in the
	 * base class.
	 * 
	 * @param streamId
	 *            The id of the source streams the partial result of which are
	 *            to be removed.
	 * 
	 */

	protected void removeEffectsOfPartialResult(int streamId) {

		// Clear the list of tuples in the appropriate stream
		partialSourceTuples[streamId].clear();
	}

	/**
	 * This function handles punctuations for the given operator. The join
	 * operator can use punctuations to purge some state.
	 * 
	 * @param tuple
	 *            The current input tuple to examine.
	 * @param streamId
	 *            The id of the source streams the partial result of which are
	 *            to be removed.
	 * 
	 */

	protected void processPunctuation(Punctuation tuple, int streamId)
			throws ShutdownException, InterruptedException {
		if (ts == null) {
			System.err
					.println("Don't know what to do with this punctuation - no punctation attr is specified");
			return;
		}

		/*
		 * Punctuation from the db side is treated as data
		 */
		if (streamId == 0) {
			processTuple(tuple, streamId);
			return;
		}

		// see if there are tuples to release from the other hash table.
		int otherStreamId = 0;

		Tuple left, right;
		left = null;
		right = tuple;

		String punctVal = ts[streamId].getAtomicValue(left, right).trim();
		if (punctVal.startsWith("("))
			punctVal = punctVal.substring(2, punctVal.length() - 1);

		// System.out.println("punctuation from: "+streamId+" on time: "+punctVal);
		long punctTS = Long.valueOf(punctVal);

		String[] values = new String[1];
		String hashKey;

		/*
		 * for each side, we assume 1) its data granularity and its punctuation
		 * granularity are the same; 2) one's granularity divides the other's
		 */
		if (granularity[otherStreamId] >= granularity[streamId]) {
			punctTS = (punctTS / granularity[0]) * granularity[0];
			values[0] = String.valueOf(punctTS);

			// Get the hash code corresponding to the tuple element
			hashKey = hashers[otherStreamId].hashKey(values);
			releaseTuple(hashKey, otherStreamId);
		} else {
			left = null;
			right = lastPunct;

			String lastPunctVal = ts[streamId].getAtomicValue(left, right)
					.trim();
			if (lastPunctVal.startsWith("("))
				lastPunctVal = lastPunctVal.substring(2,
						lastPunctVal.length() - 1);

			long lastPunctTS = Long.valueOf(lastPunctVal);

			for (long i = lastPunctTS + granularity[0]; i <= punctTS; i = i
					+ granularity[0]) {
				values[0] = String.valueOf(i);

				// Get the hash code corresponding to the tuple element
				hashKey = hashers[otherStreamId].hashKey(values);
				releaseTuple(hashKey, otherStreamId);
			}
		}

		lastPunct = tuple;
	}

	private void releaseTuple(String hashKey, int streamId)
			throws ShutdownException, InterruptedException {

		Vector hashEntry = finalSourceTuples[streamId].get(hashKey);

		if (hashEntry == null)
			return;

		// System.out.println("release tuples from stream: "+streamId+" for time: "+hashKey);

		Iterator list = hashEntry.iterator();
		while (list.hasNext()) {
			putTuple((Tuple) list.next(), 0);
			list.remove();
		}
		finalSourceTuples[streamId].remove(hashKey);

	}

	public boolean isStateful() {
		return true;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog,
	 *      LogicalProperty, LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		float leftCard = inputLogProp[0].getCardinality();
		float rightCard = inputLogProp[1].getCardinality();
		float inputCard = leftCard + rightCard;
		float outputCard = logProp.getCardinality();

		double cost = inputCard * catalog.getDouble("tuple_reading_cost");
		cost += inputCard * catalog.getDouble("tuple_hashing_cost");
		cost += outputCard * constructTupleCost(catalog);
		Cost c = new Cost(cost);
		// XXX vpapad: We must compute the predicate on all the tuple
		// combinations
		// that pass the equality predicates we're hashing on; but how do we
		// compute that? We'll just assume that's the same as the tuples that
		// appear in the output (best case)
		return c;
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#opInitialize()
	 */
	protected void opInitialize() {
		hashers = new Hasher[1];
		hashers[0] = new Hasher(punctAttrs[0]);

		hashers[0].resolveVariables(inputTupleSchemas[0]);
		hashers[0].resolveVariables(inputTupleSchemas[1]);

		granularity = new int[2];
		granularity[0] = 20;
		granularity[1] = 20;

		// Initialize the array of hash tables of partial source tuples - there
		// are
		// two input stream, so the array is of size 2
		partialSourceTuples = new DuplicateHashtable[1];

		partialSourceTuples[0] = new DuplicateHashtable();

		// Initialize the array of hash tables of final source tuples - there
		// are
		// two input stream, so the array is of size 2
		finalSourceTuples = new DuplicateHashtable[1];

		finalSourceTuples[0] = new DuplicateHashtable();

		if (punctAttrs != null) {
			ts = new SimpleAtomicEvaluator[2];

			ts[0] = new SimpleAtomicEvaluator(punctAttrs[0].get(0).getName());
			ts[1] = new SimpleAtomicEvaluator(punctAttrs[1].get(0).getName());

			ts[0].resolveVariables(inputTupleSchemas[0], 0);
			ts[1].resolveVariables(inputTupleSchemas[1], 1);

		}
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		PhysicalPresent op = new PhysicalPresent();
		op.punctAttrs = punctAttrs;
		return op;
	}

	public void setResultDocument(Document doc) {
		this.doc = doc;
	}

	public boolean equals(Object o) {
		if (o == null || !(o.getClass().equals(getClass())))
			return false;
		PhysicalPresent present = (PhysicalPresent) o;
		return equalsNullsAllowed(getLogProp(), present.getLogProp());
	}

	public int hashCode() {
		return hashCodeNullsAllowed(getLogProp());
	}

}
