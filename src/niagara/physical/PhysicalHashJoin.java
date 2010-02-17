package niagara.physical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

import niagara.connection_server.NiagraServer;
import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Join;
import niagara.logical.predicates.And;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.physical.predicates.PredicateImpl;
import niagara.utils.BaseAttr;
import niagara.utils.DuplicateHashtable;
import niagara.utils.PEException;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
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
public class PhysicalHashJoin extends PhysicalJoin {
	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false, false };

	// Optimization time structures
	/** Equijoin predicates */
	private EquiJoinPredicateList eqjoinPreds;
	/** Runtime implementation of joinPredicate */
	private PredicateImpl pred;

	private Hasher[] hashers;
	// private String[] rgstPValues;
	// private String[] rgstTValues;
	private ArrayList[] rgPunct = new ArrayList[2];

	// The array of hash tables of partial tuple elements that are read from the
	// source streams. The index of the array corresponds to the index of the
	// stream from which the tuples were read.
	DuplicateHashtable[] partialSourceTuples;

	// The array of hash tables of final tuple elements that are read from the
	// source streams. The index of the array corresponds to the index of the
	// stream from which the tuples were read.
	DuplicateHashtable[] finalSourceTuples;

	SimpleAtomicEvaluator[] ts = null;
	String[] punctAttrs = null;
	boolean[] punctAttrMatch;
	int INTERVAL = 0;
	Document doc;

	private boolean[] streamClose;

	// private ArrayList punctBuffer = null;

	public PhysicalHashJoin() {
		isSendImmediate = true;
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void initJoin(Join join) {
		eqjoinPreds = join.getEquiJoinPredicates();
		// In hash join, we hope that most tuples that hash the same
		// do indeed pass the equijoin predicates, so we put them last
		joinPredicate = And.conjunction(join.getNonEquiJoinPredicate(),
				eqjoinPreds.toPredicate());
		pred = joinPredicate.getImplementation();
		punctAttrs = join.getPunctAttrs();
		initExtensionJoin(join);

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
		// if(streamId == 0)
		// System.out.println("Physical Hash Processing tuple from stream " +
		// getStreamName(streamId));

		// Get the hash code corresponding to the tuple element
		String hashKey = hashers[streamId].hashKey(tupleElement);

		// ignore null attributes...
		if (hashKey == null)
			return;

		// String stPunctJoinKey = null;

		// Determine the id of the other stream
		int otherStreamId = 1 - streamId;

		// Now loop over all the partial elements of the other source
		// and evaluate the predicate and construct a result tuple if
		// the predicate is satisfied
		boolean producedResult;
		producedResult = constructJoinResult(tupleElement, streamId, hashKey,
				partialSourceTuples[otherStreamId]);

		/*
		 * if(producedResult) {
		 * System.out.println("Physical Hash Join Produced Results"); }
		 */
		// Extension join note: Extension join indicates that we
		// expect an extension join with referential integrity
		// meaning that tuples on the denoted extension join side
		// join only with one tuple from the other side, if I
		// join with a tuple, in partial, no need to check final
		// if I join with tuple from partial or final, no need
		// to insert in hash table

		if (!extensionJoin[streamId] || !producedResult) {
			// Now loop over all the final elements of the other source
			// and evaluate the predicate and construct a result tuple if
			// the predicate is satisfied
			producedResult = constructJoinResult(tupleElement, streamId,
					hashKey, finalSourceTuples[otherStreamId]);
		}

		Tuple left, right;

		if (streamId == 0) {
			left = tupleElement;
			right = null;
		} else {
			left = null;
			right = tupleElement;
		}

		if (!extensionJoin[streamId] || !producedResult) {
			// System.out.println("KT inserting in hash table streamID " +
			// streamId);
			// we don't add to hash table if this is extension join
			// and already joined (see above)

			// Add the tuple element to the appropriate hash table,
			// but only if we haven't yet seen a punctuation from the
			// other input that matches it
			boolean fMatch = false;

			if (streamClose[otherStreamId]) {
				fMatch = true;
			}

			long punctTS;
			Punctuation punct;
			if (ts != null && ts[streamId] != null && ts[otherStreamId] != null) {
				long timestamp = Long.valueOf(ts[streamId].getAtomicValue(left,
						right).trim());
				for (int i = 0; i < rgPunct[otherStreamId].size()
						&& fMatch == false; i++) {
					punct = (Punctuation) rgPunct[otherStreamId].get(i);
					if (otherStreamId == 0) {
						left = punct;
						right = null;
					} else {
						left = null;
						right = punct;
					}

					String punctVal = ts[otherStreamId].getAtomicValue(left,
							right).trim();
					if (punctVal.startsWith("("))
						punctVal = punctVal.substring(2, punctVal.length() - 1);

					punctTS = Long.valueOf(punctVal);

					if (punctTS >= timestamp)
						fMatch = true;
				}
			}

			if (fMatch == false)
				if (tupleElement.isPartial())
					partialSourceTuples[streamId].put(hashKey, tupleElement);
				else {
					// if(streamId == 0)
					// System.out.println("Physical Hash storing tuple from stream "
					// + getStreamName(streamId) + " hashkey: "+hashKey);

					finalSourceTuples[streamId].put(hashKey, tupleElement);
				}
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
	 * This function constructs a join result based on joining with tupleElement
	 * from the stream with id streamId with the hash table of tuples of the
	 * other stream represented by the hash table otherStreamTuples. The join
	 * results are returned in result
	 * 
	 * @param tupleElement
	 *            The tuple to be joined with tuples in other stream
	 * @param streamId
	 *            The stream id of tupleElement
	 * @param hashCode
	 *            The join hash code
	 * @param otherStreamTuples
	 *            The tuples to be joined with tupleElement
	 * @returns true if result produced (that is tupleElement joined with
	 *          something in otherStreamTuples)
	 */

	private boolean constructJoinResult(Tuple tupleElement, int streamId,
			String hashKey, DuplicateHashtable otherStreamTuples)
			throws ShutdownException, InterruptedException {

		boolean producedResult = false;

		// System.err.println("searching in stream "+getStreamName(1-streamId) +
		// "for: "+hashKey);

		// Get the list of tuple elements having the same hash code in
		// otherStreamTuples
		//
		Vector otherSourceTuples = otherStreamTuples.get(hashKey);

		// Loop over all the elements of the other source stream and
		// evaluate the predicate and construct a result tuple if the
		// predicate is satisfied
		if (otherSourceTuples == null)
			return false;

		for (int tup = 0; tup < otherSourceTuples.size();) {
			// Get the appropriate tuple for the other source stream
			Tuple otherTupleElement = (Tuple) otherSourceTuples.get(tup);

			// Make the right order for predicate evaluation
			//
			Tuple leftTuple;
			Tuple rightTuple;

			if (streamId == 0) {
				leftTuple = tupleElement;
				rightTuple = otherTupleElement;
			} else {
				leftTuple = otherTupleElement;
				rightTuple = tupleElement;
			}

			boolean removed = false;
			// Check whether the predicate is satisfied
			if (pred.evaluate(leftTuple, rightTuple)) {
				produceTuple(leftTuple, rightTuple);
				producedResult = true;
				if (extensionJoin[1 - streamId]) {
					boolean success = otherStreamTuples.remove(hashKey,
							otherTupleElement);
					if (!success)
						throw new PEException(
								"KT: removal from hash table failed");
					removed = true;
				}
				if (extensionJoin[streamId])
					return producedResult;
			}
			if (!removed)
				tup++;

		}
		return producedResult;
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

		int otherStreamId = 1 - streamId;
		Tuple left, right;
		String punctVal;
		long punctTS = Long.MIN_VALUE;
		// if the punctuating attribute is among the joining attributes of the
		// other input,
		// then this punctuation can be used to purge the state of the other
		// input;
		if (punctAttrMatch[streamId]) {

			if (streamId == 0) {
				left = tuple;
				right = null;
			} else {
				left = null;
				right = tuple;
			}

			punctVal = ts[streamId].getAtomicValue(left, right).trim();

			if (punctVal.startsWith("("))
				punctVal = punctVal.substring(2, punctVal.length() - 1);

			punctTS = Long.valueOf(punctVal);

			// see if there are tuples to remove from the other hash table.
			// check both the partial list and the final list

			Iterator keys = finalSourceTuples[otherStreamId].keySet()
					.iterator();
			Vector hashEntry;
			Iterator list;

			while (keys.hasNext()) {
				hashEntry = finalSourceTuples[otherStreamId].get(keys.next());
				list = hashEntry.iterator();
				while (list.hasNext()) {
					if (otherStreamId == 0) {
						left = (Tuple) list.next();
						right = null;
					} else {
						left = null;
						right = (Tuple) list.next();
					}

					if (Double.valueOf(ts[otherStreamId].getAtomicValue(left,
							right)) < (punctTS - INTERVAL)) {

						// System.out.println("Physical Hash purging tuple from stream "
						// + getStreamName(otherStreamId));

						list.remove();
					}
				}
				if (hashEntry.size() == 0)
					keys.remove();
			}
		}

		// then, produce punctuation and add this punctuation
		// to the appropriate list of punctuations if necessary

		// First, if the other stream is closed, it is equivalent to
		// an all-star (matching everything) punctuation;
		if (streamClose[otherStreamId]) {
			passOnPunct(tuple, streamId);
			return;
		}

		// Second, it the other stream isn't end, let's check whether a matching
		// punctuation has arrived on it;
		Iterator it = rgPunct[otherStreamId].iterator();
		long otherTs;
		boolean fMatch = false;

		while (it.hasNext() && !fMatch) {
			// We assume linear punctuation of equal granularity on both input
			// streams,
			// e.g., punctuation every minute on both input streams.
			if (otherStreamId == 0) {
				left = (Punctuation) it.next();
				right = null;
			} else {
				left = null;
				right = (Punctuation) it.next();
			}
			punctVal = ts[otherStreamId].getAtomicValue(left, right).trim();
			if (punctVal.startsWith("("))
				punctVal = punctVal.substring(2, punctVal.length() - 1);

			otherTs = Long.valueOf(punctVal);

			if ((punctTS - INTERVAL) == otherTs) {
				if (streamId == 0)
					producePunctuation(tuple, (Punctuation) right, 1);
				else
					producePunctuation((Punctuation) left, tuple, 0);
				it.remove();
			} else if ((otherTs - INTERVAL) == punctTS) {
				if (streamId == 0)
					producePunctuation(tuple, (Punctuation) right, 0);
				else
					producePunctuation((Punctuation) left, tuple, 1);

				fMatch = true;
			}
		}
		if (!fMatch)
			rgPunct[streamId].add(tuple);
	}

	/*
	 * protected void processPunctuation(Punctuation tuple, int streamId) throws
	 * ShutdownException, InterruptedException {
	 * 
	 * try { //first, add this to the appropriate list of punctuations String
	 * stPunctKey = hashers[streamId].hashKey(tuple);
	 * rgPunct[streamId].add(tuple);
	 * 
	 * //now, see if there are tuples to remove from the other hash table. //
	 * check both the partial list and the final list
	 * hashers[streamId].getValuesFromKey(stPunctKey, rgstPValues); int
	 * otherStreamId = 1-streamId;
	 * 
	 * purgeHashTable(partialSourceTuples[otherStreamId], otherStreamId,
	 * rgstPValues); purgeHashTable(finalSourceTuples[otherStreamId],
	 * otherStreamId, rgstPValues); } catch
	 * (java.lang.ArrayIndexOutOfBoundsException ex) { //Not a punctuation for
	 * the join attribute. Ignore it. ; }
	 * 
	 * return; }
	 */

	/**
	 * @param tuple
	 * @param streamId
	 * 
	 *            No punctuating attribute is specified. We pass on punctuation
	 *            tuples directly to the output.
	 * 
	 */
	private void passOnPunct(Punctuation tuple, int streamId)
			throws InterruptedException, ShutdownException {
		putPunctuation(tuple, streamId);
		/*
		 * if (streamClose[1-streamId]) putPunctuation(tuple, streamId); else {
		 * if (punctBuffer == null) punctBuffer = new ArrayList();
		 * punctBuffer.add(tuple); }
		 */
	}

	protected void producePunctuation(Punctuation left, Punctuation right,
			int streamId) throws ShutdownException, InterruptedException {
		Punctuation result;

		if (NiagraServer.DEBUG)
			System.err.println("hash join producing punctuation on stream "
					+ streamId);
		if (projecting) {
			result = left.copy(outputTupleSchema.getLength(), leftAttributeMap);
			right.copyInto(result, leftAttributeMap.length, rightAttributeMap);
		} else {
			result = left.copy(outputTupleSchema.getLength());
			result.appendTuple(right);
		}

		if (streamId == 0) { // left side punctuation;
			int pos = outputTupleSchema.getPosition(ts[0].getName());
			result.setAttribute(pos, BaseAttr
					.createWildStar(BaseAttr.Type.String));
			// Element n = doc.createElement(ts[1].getName());
			// n.appendChild(doc.createTextNode("*"));
			// result.setAttribute(pos, n);
		} else { // right side punctuation;
			int pos = outputTupleSchema.getPosition(ts[1].getName());
			result.setAttribute(pos, BaseAttr
					.createWildStar(BaseAttr.Type.String));
			// Element n = doc.createElement(ts[0].getName());
			// n.appendChild(doc.createTextNode("*"));
			// result.setAttribute(pos, n);
		}

		// Add the result to the output
		putTuple(result, 0);
	}

	// private void purgeHashTable(DuplicateHashtable ht, int streamId,
	// String[] rgstPunct) {
	// Enumeration enKeys = ht.keys();
	// while (enKeys.hasMoreElements()) {
	// String hashKey = (String) enKeys.nextElement();
	// hashers[streamId].getValuesFromKey(hashKey, rgstTValues);
	//
	// boolean fMatch = true;
	// for (int i = 0; i < rgstPunct.length && fMatch == true; i++) {
	// fMatch = Punctuation.matchValue(rgstPunct[i], rgstTValues[i]);
	// }
	//
	// if (fMatch) {
	// // System.out.println("purging in stream " +
	// // getStreamName(streamId) +" for "+hashKey);
	// ht.remove(hashKey);
	// }
	// }
	// }

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
		c.add(pred.getCost(catalog).times(outputCard));
		return c;
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#opInitialize()
	 */
	protected void opInitialize() {
		Attrs leftAttrs = eqjoinPreds.getLeft();
		Attrs rightAttrs = eqjoinPreds.getRight();

		hashers = new Hasher[2];
		hashers[0] = new Hasher(leftAttrs);
		hashers[1] = new Hasher(rightAttrs);

		// rgstPValues = new String[leftAttrs.size()];
		// rgstTValues = new String[rightAttrs.size()];

		pred.resolveVariables(inputTupleSchemas[0], 0);
		pred.resolveVariables(inputTupleSchemas[1], 1);

		hashers[0].resolveVariables(inputTupleSchemas[0]);
		hashers[0].resolveVariables(inputTupleSchemas[1]);
		hashers[1].resolveVariables(inputTupleSchemas[0]);
		hashers[1].resolveVariables(inputTupleSchemas[1]);

		// Initialize the array of hash tables of partial source tuples - there
		// are
		// two input stream, so the array is of size 2
		partialSourceTuples = new DuplicateHashtable[2];

		partialSourceTuples[0] = new DuplicateHashtable();
		partialSourceTuples[1] = new DuplicateHashtable();

		// Initialize the array of hash tables of final source tuples - there
		// are
		// two input stream, so the array is of size 2
		finalSourceTuples = new DuplicateHashtable[2];

		finalSourceTuples[0] = new DuplicateHashtable();
		finalSourceTuples[1] = new DuplicateHashtable();

		// Initialize the punctuation lists
		rgPunct[0] = new ArrayList();
		rgPunct[1] = new ArrayList();

		punctAttrMatch = new boolean[2];
		punctAttrMatch[0] = false;
		punctAttrMatch[1] = false;

		if (punctAttrs != null) {
			ts = new SimpleAtomicEvaluator[2];

			for (int i = 0; i < punctAttrs.length; i++) {
				if (punctAttrs[i].compareTo("") != 0) {
					ts[i] = new SimpleAtomicEvaluator(punctAttrs[i]);
					ts[i].resolveVariables(inputTupleSchemas[i], i);

					Attrs joiningAttrs;

					// Is the punctuation attribute among the joining
					// attributes?
					if (i == 0)
						joiningAttrs = leftAttrs;
					else
						joiningAttrs = rightAttrs;

					if (joiningAttrs.getAttr(punctAttrs[i]) != null)
						punctAttrMatch[i] = true;

				}
			}
		}

		streamClose = new boolean[2];
		streamClose[0] = false;
		streamClose[1] = false;
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		PhysicalHashJoin op = new PhysicalHashJoin();
		op.pred = pred;
		op.joinPredicate = joinPredicate;
		op.eqjoinPreds = eqjoinPreds;
		op.extensionJoin = extensionJoin;
		op.punctAttrs = punctAttrs;
		op.ts = ts;
		op.streamClose = streamClose;
		op.punctAttrMatch = punctAttrMatch;
		return op;
	}

	public void setResultDocument(Document doc) {
		this.doc = doc;
	}

	public void getInstrumentationValues(
			ArrayList<String> instrumentationNames,
			ArrayList<Object> instrumentationValues) {
		if (instrumented) {
			instrumentationNames.add("left hashtable size");
			if (finalSourceTuples == null)
				instrumentationValues.add(0);
			else
				instrumentationValues.add(finalSourceTuples[0].size());

			instrumentationNames.add("right hashtable size");
			if (finalSourceTuples == null)
				instrumentationValues.add(0);
			else
				instrumentationValues.add(finalSourceTuples[1].size());
			super.getInstrumentationValues(instrumentationNames,
					instrumentationValues);
		}
	}

	public void streamClosed(int streamId) throws ShutdownException {
		// stream close is equivalent to an all-star, matching everything
		// punctuation; it purges all the tuples on the other input, and allows
		// output of all the punctuations on the other input;
		int otherStreamId = 1 - streamId;
		streamClose[streamId] = true;
		finalSourceTuples[otherStreamId].clear();
		Iterator puncts = rgPunct[otherStreamId].iterator();
		try {
			while (puncts.hasNext()) {
				putPunctuation((Punctuation) puncts.next(), otherStreamId);
				puncts.remove();
			}
		} catch (InterruptedException e) {
			System.err
					.println("Non-interger punctuation value found in HashJoin; don't know what to do");
			return;
		}
	}

	private void putPunctuation(Punctuation tuple, int streamId)
			throws InterruptedException, ShutdownException {
		Punctuation result;
		Punctuation left, right;

		if (streamId == 0) {
			left = tuple;
			right = null;
		} else {
			right = tuple;
			left = null;
		}

		if (projecting) {
			if (left == null) {
				result = new Punctuation(false);
				for (int i = 0; i < leftAttributeMap.length; i++)
					result.appendAttribute(new StringAttr("*"));
			} else
				result = left.copy(outputTupleSchema.getLength(),
						leftAttributeMap);

			if (right == null) {
				for (int i = 0; i < rightAttributeMap.length; i++)
					result.appendAttribute(new StringAttr("*"));
			} else
				right.copyInto(result, leftAttributeMap.length,
						rightAttributeMap);
		} else {
			int length;
			if (left == null) {
				result = new Punctuation(false);
				length = inputTupleSchemas[0].getLength();
				for (int i = 0; i < length; i++)
					result.appendAttribute(new StringAttr("*"));
			} else
				result = left.copy(outputTupleSchema.getLength());

			if (right == null) {
				length = inputTupleSchemas[1].getLength();
				for (int i = 0; i < length; i++)
					result.appendAttribute(new StringAttr("*"));
			} else
				result.appendTuple(right);
		}
		putTuple(result, 0);
	}
}
