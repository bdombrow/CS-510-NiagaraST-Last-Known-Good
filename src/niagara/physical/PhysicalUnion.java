package niagara.physical;

import java.lang.reflect.Array;
import java.util.ArrayList;

import niagara.logical.Union;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * <code>PhysicalUnion</code> implements a union of a set of incoming streams;
 * 
 * @see PhysicalOperator
 */
@SuppressWarnings("unchecked")
public class PhysicalUnion extends PhysicalOperator {
	private ArrayList[] rgPunct;
	private int[] rgnRemove;
	private Attrs[] inputAttrs;
	private int[][] attributeMaps;
	private boolean hasMappings;
	private int outSize;

	private int tupleCounter = 0;
	// private int leftCounter = 0;
	// private int rightCounter = 0;
	boolean slowStream = true;

	public PhysicalUnion() {
		// XXX vpapad: here we have to initialize blockingSourceStreams
		// but we don't know how many input streams we have yet.
		// We postpone it until initFrom - is that too late?
		// KT - I think that should be ok, blockingSourceStreams
		// isn't used until execution - I think...
	}

	public PhysicalUnion(int arity) {
		setBlockingSourceStreams(new boolean[arity]);
		// setBlockingSourceStreams(false false);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		Union logicalOp = (Union) logicalOperator;

		setBlockingSourceStreams(new boolean[logicalOp.getArity()]);
		hasMappings = false;
		if (logicalOp.numMappings() > 0)
			hasMappings = true;
		inputAttrs = logicalOp.getInputAttrs();

		assert logicalOp.getArity() == Array.getLength(inputAttrs) : "Arity doesn't match num input attrs ";
	}

	public void opInitialize() {
		// XXX vpapad: really ugly...
		setBlockingSourceStreams(new boolean[numSourceStreams]);
		rgPunct = new ArrayList[getArity()];
		for (int i = 0; i < rgPunct.length; i++)
			rgPunct[i] = new ArrayList();

		rgnRemove = new int[getArity()];
	}

	/**
	 * This function processes a tuple element read from a source stream when
	 * the operator is non-blocking. This over-rides the corresponding function
	 * in the base class.
	 * 
	 * @param inputTuple
	 *            The tuple element read from a source stream
	 * @param streamId
	 *            The source stream from which the tuple was read
	 * 
	 * @exception ShutdownException
	 *                query shutdown by user or execution error
	 */

	/*
	 * This is Case B
	 * 
	 * protected void processTuple(Tuple inputTuple, int streamId) throws
	 * ShutdownException, InterruptedException {
	 * 
	 * if (streamId == 0) leftCounter++; else rightCounter++;
	 * 
	 * //tupleCounter++;
	 * 
	 * if ((leftCounter - rightCounter) == 61) { // send message
	 * sendCtrlMsgUpStream(CtrlFlags.MESSAGE, "From Union, With Love", 1); //
	 * reset counters leftCounter = 0; rightCounter = 0;
	 * //System.err.println("UNION sent the message"); //Thread.sleep(100); }
	 * 
	 * tupleCounter++; long time = System.currentTimeMillis();
	 * System.out.println(tupleCounter + "," + time); if (hasMappings) { // We
	 * need to move some attributes putTuple(inputTuple.copy(outSize,
	 * attributeMaps[streamId]), 0); } else { // just send the original tuple
	 * along putTuple(inputTuple, 0); } }
	 */

	/*
	 * This is case A
	 * 
	 * protected void processTuple(Tuple inputTuple, int streamId) throws
	 * ShutdownException, InterruptedException {
	 * 
	 * if (streamId == 0) leftCounter++; else rightCounter++;
	 * 
	 * //tupleCounter++;
	 * 
	 * if (slowStream) { if ((leftCounter - rightCounter) == 61) { slowStream =
	 * false; } }
	 * 
	 * if (streamId == 0) { tupleCounter++; long time =
	 * System.currentTimeMillis(); System.out.println(tupleCounter + "," +
	 * time); if (hasMappings) { // We need to move some attributes
	 * putTuple(inputTuple.copy(outSize, attributeMaps[streamId]), 0); } else {
	 * // just send the original tuple along putTuple(inputTuple, 0); } }
	 * 
	 * else if (slowStream) { tupleCounter++; long time =
	 * System.currentTimeMillis(); System.out.println(tupleCounter + "," +
	 * time); if (hasMappings) { // We need to move some attributes
	 * putTuple(inputTuple.copy(outSize, attributeMaps[streamId]), 0); } else {
	 * // just send the original tuple along putTuple(inputTuple, 0); } } }
	 */

	/* This is the code that generated the first output (with timers, etc.) */
	protected void processTuple(Tuple inputTuple, int streamId)
			throws ShutdownException, InterruptedException {
		// XXX: RJFM
		tupleCounter++;
		long time = System.currentTimeMillis();
		System.out.println(tupleCounter + "," + time);
		if (hasMappings) { // We need to move some attributes
			putTuple(inputTuple.copy(outSize, attributeMaps[streamId]), 0);
		} else {
			// just send the original tuple along
			putTuple(inputTuple, 0);

		}
	}

	/**
	 * This function handles punctuations for the given operator. For Union, we
	 * have to make sure all inputs have reported equal punctuation before
	 * outputting a punctuation.
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

		boolean fAllMatch = true, fFound;

		// First, check to see if this punctuation matches a punctuation
		// from all other inputs
		for (int i = 0; i < rgPunct.length && fAllMatch == true; i++) {
			if (i != streamId) {
				fFound = false;
				for (int j = 0; j < rgPunct[i].size() && fFound == false; j++) {
					fFound = tuple.equals((Punctuation) rgPunct[i].get(j));
					if (fFound)
						rgnRemove[i] = j;
				}
				fAllMatch = fFound;
			}
		}

		if (fAllMatch) {
			// Output the punctuation
			putTuple(tuple, 0);
			// Remove the other punctuations, since they are no longer needed
			for (int i = 0; i < rgPunct.length; i++) {
				if (i != streamId)
					rgPunct[i].remove(rgnRemove[i]);
			}
		} else {
			rgPunct[streamId].add(tuple);
		}
	}

	public boolean isStateful() {
		return false;
	}

	/**
	 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty,
	 *      LogicalProperty[])
	 */
	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		double trc = catalog.getDouble("tuple_reading_cost");
		double sumCards = 0;
		for (int i = 0; i < inputLogProp.length; i++)
			sumCards += inputLogProp[i].getCardinality();
		return new Cost(trc * sumCards);
	}

	/**
	 * @see niagara.optimizer.colombia.Op#copy()
	 */
	public Op opCopy() {
		PhysicalUnion newOp = new PhysicalUnion(getArity());
		newOp.inputAttrs = inputAttrs;
		newOp.attributeMaps = attributeMaps;
		newOp.hasMappings = hasMappings;
		newOp.outSize = outSize;
		return newOp;
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object o) {
		if (o == null || !(o instanceof PhysicalUnion))
			return false;
		if (o.getClass() != PhysicalUnion.class)
			return o.equals(this);
		return getArity() == ((PhysicalUnion) o).getArity()
				&& inputAttrs.equals(((PhysicalUnion) o).inputAttrs);
	}

	public int hashCode() {
		if (hasMappings)
			return getArity() ^ inputAttrs.hashCode();
		else
			return getArity();
	}

	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		super.constructTupleSchema(inputSchemas);
		outSize = outputTupleSchema.getLength();

		// if no mapping is specified input schemas must have
		// same length and that length is same as length of output schema
		if (hasMappings) {
			int inputArity = Array.getLength(inputAttrs);

			assert inputArity == Array.getLength(inputSchemas) : " input arity not equal to number of input schemas";

			attributeMaps = new int[inputArity][];
			for (int i = 0; i < inputArity; i++) {
				attributeMaps[i] = new int[outSize];
				for (int j = 0; j < outSize; j++) {
					if (inputAttrs[i] == null) {
						attributeMaps[i][j] = -1;
					} else {
						Attribute a = inputAttrs[i].GetAt(j);
						if (a == null) {
							attributeMaps[i][j] = -1;
						} else {
							attributeMaps[i][j] = inputSchemas[i].getPosition(a
									.getName());
						}
					}
				}
			}
		}
	}
}
