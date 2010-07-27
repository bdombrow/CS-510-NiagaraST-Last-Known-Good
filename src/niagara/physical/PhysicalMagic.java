package niagara.physical;

import niagara.logical.Magic;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.physical.predicates.PredicateImpl;
import niagara.query_engine.TupleSchema;
import niagara.utils.ControlFlag;
import niagara.utils.IntegerAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * 
 * Magic operator RJFM 2008.09.23 Used to drive the CIDR query (simulates
 * sending feedback every fixed time interval.)
 */

public class PhysicalMagic extends PhysicalOperator {
	// No blocking source streams
	private static final boolean[] blockingSourceStreams = { false };

	// Propagate?
	private boolean propagate;

	// Interval
	private long interval;

	// Attribute to punctuate on
	private Attribute pAttr;

	// Attribute to read time from
	private Attribute tAttr;

	// The is the predicate to apply to the tuples
	private Predicate pred;
	private PredicateImpl predEval;

	// State
	private int watermark = 0;
	private int segment = 1;

	// Granularity of window id (can we get this dynamically?)
	private int widInMillis = 60000;

	public PhysicalMagic() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		pred = ((Magic) logicalOperator).getPredicate();
		predEval = pred.getImplementation();
		interval = ((Magic) logicalOperator).getInterval();
		pAttr = ((Magic) logicalOperator).getPunctAttr();
		tAttr = ((Magic) logicalOperator).getTimeAttr();
		propagate = ((Magic) logicalOperator).getPropagate();

	}

	public Op opCopy() {
		PhysicalMagic p = new PhysicalMagic();
		p.pred = pred;
		p.predEval = predEval;
		p.interval = interval;
		p.pAttr = pAttr;
		p.tAttr = tAttr;
		p.propagate = propagate;
		return p;
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

	private int nextSegment(int current) {
		if (current == 8)
			return 1;
		return current + 1;

	}

	protected void processTuple(Tuple inputTuple, int streamId)
			throws ShutdownException, InterruptedException {
		// don't actually evaluate the predicate (this is inherited from select
		// and will go away)
		// Evaluate the predicate on the desired attribute of the tuple
		// if (predEval.evaluate(inputTuple, null))

		/** This is the magic! */

		// read parameters
		int pos = inputTupleSchemas[0].getPosition(tAttr.getName());
		IntegerAttr t = (IntegerAttr) inputTuple.getAttribute(pos);
		int tval = Integer.parseInt(t.toASCII());

		if ((tval * widInMillis) - watermark >= interval) {
			// change segment
			segment = nextSegment(segment);
			watermark += interval;
			// send feedback: don't need to see other segments
			String msg = pAttr + "#" + segment;
			if (propagate) {
				sendCtrlMsgUpStream(ControlFlag.MESSAGE, msg, 0, null);
				System.err.println(this.getName() + " sent message: " + msg);
			}
		}

		putTuple(inputTuple, 0);
		// System.err.println("Currently in segment: " + segment);
		// System.err.println("interval: " + interval + " punctattr: " +
		// pAttr.getName() + " timeattr: " + tAttr.getName() + " := " + tval);
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
		if (o == null || !(o instanceof PhysicalMagic))
			return false;
		if (o.getClass() != PhysicalMagic.class)
			return o.equals(this);
		if (interval != ((PhysicalMagic) o).interval)
			return false;
		if (pAttr.getName() != ((PhysicalMagic) o).pAttr.getName())
			return false;
		if (tAttr.getName() != ((PhysicalMagic) o).tAttr.getName())
			return false;
		if (propagate != ((PhysicalMagic) o).propagate)
			return false;
		return pred.equals(((PhysicalMagic) o).pred);
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		int p = 0;
		if (propagate)
			p = 1;

		return pred.hashCode() ^ (int) interval ^ pAttr.hashCode()
				^ tAttr.hashCode() ^ p;
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
