package niagara.physical;

import java.util.ArrayList;
import java.util.StringTokenizer;

import niagara.logical.Impute;
import niagara.logical.PunctSpec;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.OperatorDoneException;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.TSAttr;
import niagara.utils.Tuple;
import niagara.utils.XMLAttr;

/**
 * <code>PhysicalImpute</code> implements a punctuate operator for an incoming
 * stream;
 * 
 * @see PhysicalOperator
 */
public class PhysicalImpute extends PhysicalOperator {

	// XXX RJFM: This is an array of Strings used as a control message for
	// DBThread.
	private String attributesFromStream;

	// operator does not block on any source streams
	private static final boolean[] blockingSourceStreams = { false, false };

	// two input streams - one is data from db
	// second is a stream
	// assume index of dbdata is 0
	// index of stream is 1

	// attribute to punctuate on
	private Attribute pAttr;

	private PunctSpec pSpec;

	// private SimilaritySpec sSpec;
	// private PrefetchSpec pfSpec;

	// punctuating attribute of the input stream, which we rely
	// on to retrieve data from db
	private Attribute spAttr;

	private int[] pIdx; // index of punctuation attr

	// last timestamp - for on change punctuation
	// private long lastts;

	// keep track of data types to be used to create
	// punctuation
	BaseAttr.Type dataType[];

	// queryInterval should specify the coverage of each database query;
	// private int queryGranularity = 1*20; //each database query covers 100
	// second data;

	// the time attribute of db data
	private String timeAttr;

	private String queryString;

	private Long lastPunct = Long.MIN_VALUE;

	// private int count = 0;
	// high watermark on db data;
	// private long highWatermark;

	public PhysicalImpute() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	// probably should take punct index instead
	// index vs attr -get attr in load from xml, probably
	// can't get index there
	// probably take attr here, and convert to idx in
	// opInitialize
	public PhysicalImpute(Attribute pAttr, String timeAttr, String queryString,
			PunctSpec pSpec) {
		setBlockingSourceStreams(blockingSourceStreams);

		this.pAttr = pAttr;
		this.pSpec = pSpec;
		// this.sSpec = sSpec;
		// this.pfSpec = pfSpec;
		this.timeAttr = timeAttr;
		this.queryString = queryString;
	}

	public void setSPAttr(Attribute spAttr) {
		this.spAttr = spAttr;
	}

	/***
	 * 
	 * Specialization
	 * 
	 * @param ctrl
	 * @param streamId
	 * @throws java.lang.InterruptedException
	 * @throws ShutdownException
	 */

	/*
	 * Case B void processCtrlMsgFromSink(ArrayList ctrl, int streamId) throws
	 * java.lang.InterruptedException, ShutdownException { // downstream control
	 * message is GET_PARTIAL // We should not get SYNCH_PARTIAL, END_PARTIAL,
	 * EOS or NULLFLAG // REQ_BUF_FLUSH is handled inside SinkTupleStream //
	 * here (SHUTDOWN is handled with exceptions)
	 * 
	 * if (ctrl == null) return;
	 * 
	 * int ctrlFlag =(Integer) ctrl.get(0);
	 * 
	 * switch (ctrlFlag) { case CtrlFlags.GET_PARTIAL:
	 * processGetPartialFromSink(streamId); break; case CtrlFlags.MESSAGE:
	 * //System.err.println(this.getName() + " got message: " + ctrl.get(1));
	 * sendCtrlMsgUpStream(CtrlFlags.MESSAGE, "From PunctQC, With Love", 0);
	 * break; default: assert false : "KT unexpected control message from sink "
	 * + CtrlFlags.name[ctrlFlag]; } }
	 */

	public void opInitFrom(LogicalOp logicalOperator) {
		Impute pop = (Impute) logicalOperator;
		pAttr = pop.getPunctAttr();
		pSpec = pop.getPunctSpec();
		// sSpec = pop.getSimilaritySpec();
		// pfSpec = pop.getPrefetchSpec();
		spAttr = pop.getStreamPunctAttr();
		timeAttr = pop.getTimeAttr();
		queryString = pop.getQueryString();

	}

	public void opInitialize() {
		// initialize ts to -1 so we can detect the first
		// tuple
		// lastts = Long.MIN_VALUE;

		pIdx = new int[2];

		// get index of attribute we are punctuating on
		pIdx[0] = inputTupleSchemas[0].getPosition(pAttr.getName());
		pIdx[1] = inputTupleSchemas[1].getPosition(spAttr.getName());

		// get the data types of all attributes
		int numAttrs = inputTupleSchemas[0].getLength();
		dataType = new BaseAttr.Type[numAttrs];
		for (int i = 0; i < numAttrs; i++) {
			dataType[i] = inputTupleSchemas[0].getVariable(i).getDataType();
		}

		// print to verify i've got the inputs set up right
		System.out.println("Impute - pidx[0]: " + pIdx[0]);
		System.out.println("Impute - pidx[1]: " + pIdx[1]);
		System.out.println("Impute - len input 0 (db): "
				+ inputTupleSchemas[0].getLength());
		System.out.println("Impute - first attr input 0: "
				+ inputTupleSchemas[0].getVariable(0).getName());
		System.out.println("Impute - len input 1 (stream punct): "
				+ inputTupleSchemas[1].getLength());
		System.out.println("Impute - first attr input 1: "
				+ inputTupleSchemas[1].getVariable(0).getName());
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
	 *                query shutdown by user or<punctqc id = "pqc"
	 *                timeattr="starttime" punctattr="panetime time_t" puncttype
	 *                = "onchange" input="archive dup"/> execution error
	 */
	protected void processTuple(Tuple inputTuple, int streamId)

	throws ShutdownException, InterruptedException, OperatorDoneException {

		if (streamId == 1) {

			/* XXX RJFM get values from stream to form the Control Message */

			// Encode the incoming tuple in a single string to use as control
			// message.
			String ctrlMessage = "";

			attributesFromStream = ((XMLAttr) inputTuple.getAttribute(1))
					.toASCII();

			StringTokenizer token = new StringTokenizer(attributesFromStream,
					"\n");
			while (token.hasMoreTokens()) {
				ctrlMessage = ctrlMessage
						+ token.nextElement().toString().trim() + "#";
			}

			ControlFlag ctrlFlag = ControlFlag.CHANGE_QUERY;
			sendCtrlMsgUpStream(ctrlFlag, ctrlMessage, 0); // sent to DBThread
			// (id 0)

		} else {

			/* XXX RJFM Received tuple from dbthread, pass it downstream. */

			putTuple(inputTuple, 0); // Output DownStream has only one id, which
			// is 0.
		}
	}

	/*
	 * Assume timestamp is in seconds;
	 */
	private long getTupleTimestamp(Tuple inputTuple, int streamId) {
		assert (inputTuple.getAttribute(pIdx[streamId])).getClass() == TSAttr.class : "bad punct attr type";
		TSAttr tsAttr = (TSAttr) inputTuple.getAttribute(pIdx[streamId]);
		return tsAttr.extractEpoch();
	}

	/**
	 * This function generates a punctuation based on the last ts value using
	 * the template generated by setupDataTemplate
	 */

	/**
	 * This function handles punctuations for the given operator. For Punctuate,
	 * we can simply output any incoming punctuation.
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

		// FIX - process punct from stream
		assert streamId == 1 : "Shouldn't get punct from db side";
		if (streamId == 0) {
			System.err
					.println("DO SOMETHING HERE - PROCESS PUNCT FROM DB SIDE");
			return;
		}

		long ts = getTupleTimestamp(tuple, 1);
		synchronized (lastPunct) {
			lastPunct = ts;
		}

		/* XXX RJFM */
		// int ctrlFlag = CtrlFlags.CHANGE_QUERY;

		// sendCtrlMsgUpStream(ctrlFlag, String.valueOf(ts), 0);

		/*
		 * int ctrlFlag = CtrlFlags.IMPUTE; sendCtrlMsgUpStream(ctrlFlag, "a",
		 * 0);
		 */

		/*
		 * long start, end;
		 * 
		 * int prefetch = pfSpec.getPrefetchVal();
		 * 
		 * if (highWatermark < ts+prefetch) { int queryCoverage =
		 * pfSpec.getCoverage();
		 * 
		 * if (count == 0) { start = ts; //start = ts - sSpec.getNumOfMins()*60;
		 * count++; } else start = highWatermark; end = (start + queryCoverage);
		 * 
		 * String ctrlMsg = start + " " + end; //newQuery(start, end);
		 * 
		 * System.err.println(ctrlMsg); highWatermark = end; int ctrlFlag =
		 * CtrlFlags.CHANGE_QUERY;
		 * 
		 * sendCtrlMsgUpStream(ctrlFlag, ctrlMsg, 0); }
		 */
	}

	public void streamClosed(int streamId) throws ShutdownException {
		try {
			if (streamId == 1) // the stream ends;
				// send a READY_TO_FINISH msg to its left input source, which is
				// the dbthread;
				System.err
						.println("the stream is going to end. Sending out the Shutdown msg ..");
			sendCtrlMsgUpStream(ControlFlag.READY_TO_FINISH, null, 0);
		} catch (InterruptedException e) {
			;
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
		PhysicalImpute another = new PhysicalImpute(pAttr, timeAttr,
				queryString, pSpec);
		another.setSPAttr(spAttr);
		return another;
	}

	// HERE
	/**
	 * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	 */
	public void constructTupleSchema(TupleSchema[] inputSchemas) {
		inputTupleSchemas = inputSchemas;
		outputTupleSchema = inputTupleSchemas[0];
	}

	/**
	 * @see java.lang.Object#equals(Object)
	 */
	public boolean equals(Object o) {
		// FIX - this is garbage
		if (o == null || !(o instanceof PhysicalImpute))
			return false;
		if (o.getClass() != PhysicalImpute.class)
			return o.equals(this);
		return getArity() == ((PhysicalPunctuate) o).getArity();
	}

	public int hashCode() {
		// FIX - this is garbage
		return getArity(); // what is this ??
	}

	public void getInstrumentationValues(
			ArrayList<String> instrumentationNames,
			ArrayList<Object> instrumentationValues) {
		instrumentationNames.add("now");
		synchronized (lastPunct) {
			instrumentationValues.add(String.valueOf(lastPunct));
		}

		// super.getInstrumentationValues(instrumentationNames,
		// instrumentationValues);
	}
}
