package niagara.physical;

import java.util.HashMap;
import java.util.Vector;

import niagara.logical.IncrementalGroup;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;
import niagara.xmlql_parser.skolem;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/***
 * Implementation of IncrementalGroup
 * */
@SuppressWarnings("unchecked")
public abstract class PhysicalIncrementalGroup extends PhysicalOperator {
	// No blocking input streams
	private static final boolean[] blockingSourceStreams = { false };

	// The logical operator for grouping
	protected IncrementalGroup logicalGroupOperator;

	// The list of attributes to group by
	private int[] groupAttrs;

	private Hasher hasher;

	/** Keeps final per-group information */
	private HashMap hash2final;
	/** Keeps partial per-group information */
	private HashMap hash2partial;
	/** Keeps a representative tuple for each group */
	private HashMap hash2tuple;

	protected Document doc;

	public PhysicalIncrementalGroup() {
		setBlockingSourceStreams(blockingSourceStreams);
	}

	public void opInitFrom(LogicalOp logicalOperator) {
		// Typecast to a group logical operator
		logicalGroupOperator = (IncrementalGroup) logicalOperator;
	}

	/**
	 * This function initializes the data structures for an operator. This
	 * over-rides the corresponding function in the base class.
	 * 
	 * @return True if the operator is to continue and false otherwise
	 */
	protected void opInitialize() {
		// Get the grouping attributes
		skolem grouping = logicalGroupOperator.getSkolemAttributes();

		Vector groupVars = grouping.getVarList();
		hasher = new Hasher(groupVars);

		groupAttrs = new int[groupVars.size()];
		for (int i = 0; i < groupAttrs.length; i++) {
			groupAttrs[i] = inputTupleSchemas[0]
					.getPosition(((Attribute) groupVars.get(i)).getName());
		}

		hasher.resolveVariables(inputTupleSchemas[0]);

		// Initialize the hash tables
		hash2final = new HashMap();
		hash2partial = new HashMap();
		hash2tuple = new HashMap();
	}

	/**
	 * @see niagara.query_engine.PhysicalOperator#processTuple(Tuple, int)
	 */
	protected void processTuple(Tuple tuple, int streamId)
			throws ShutdownException, InterruptedException {
		String hash = hasher.hashKey(tuple);

		// ignore null attributes...
		if (hash == null)
			return;

		// Have we seen this group before?
		Tuple representativeTuple = (Tuple) hash2tuple.get(hash);
		if (representativeTuple == null) {
			// Output tuples contain the groupby attributes plus
			// two more fields: previous and current group info
			representativeTuple = new Tuple(true, groupAttrs.length + 2);
			// Copy the groupby attributes
			for (int i = 0; i < groupAttrs.length; i++) {
				representativeTuple.appendAttribute(tuple
						.getAttribute(groupAttrs[i]));
			}
			// Initialize the group hashtable entries
			hash2partial.put(hash, emptyGroupValue());
			hash2final.put(hash, emptyGroupValue());
			hash2tuple.put(hash, representativeTuple);
		}

		Tuple newTuple = new Tuple(tuple.isPartial(), groupAttrs.length + 2);
		newTuple.appendTuple(representativeTuple);

		Node oldResult = null;
		Node newResult;
		// partial tuples only affect partial results
		Object oldGroupInfo = hash2partial.get(hash);
		if (logicalGroupOperator.outputOldValue())
			oldResult = constructOutput(oldGroupInfo);
		Object newGroupInfo = processTuple(tuple, oldGroupInfo);
		newResult = constructOutput(newGroupInfo);
		hash2partial.put(hash, newGroupInfo);
		// final tuples affect both final and partial results
		if (!tuple.isPartial()) {
			// final tuples affect both final and partial results
			oldGroupInfo = hash2final.get(hash);
			if (logicalGroupOperator.outputOldValue())
				oldResult = constructOutput(oldGroupInfo);
			newGroupInfo = processTuple(tuple, hash2final.get(hash));
			newResult = constructOutput(newGroupInfo);
			hash2final.put(hash, newGroupInfo);
		}
		if (newGroupInfo == oldGroupInfo) {
			// There was no change in the group,
			// do not output a tuple
			return;
		}
		if (logicalGroupOperator.outputOldValue())
			newTuple.appendAttribute(oldResult);
		newTuple.appendAttribute(newResult);
		putTuple(newTuple, 0);
	}

	protected final void removeEffectsOfPartialResult(int streamId) {
		// Rewind partial results to the final results
		hash2partial.putAll(hash2final);
	}

	public void setResultDocument(Document doc) {
		this.doc = doc;
	}

	public boolean isStateful() {
		return true;
	}

	public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
		// XXX vpapad: really naive. Only considers the hashing cost
		float inpCard = inputLogProp[0].getCardinality();
		float outputCard = logProp.getCardinality();

		double cost = inpCard * catalog.getDouble("tuple_reading_cost");
		cost += inpCard * catalog.getDouble("tuple_hashing_cost");
		cost += outputCard * catalog.getDouble("tuple_construction_cost");
		return new Cost(cost);
	}

	public abstract Object processTuple(Tuple tuple, Object previousGroupInfo);

	public abstract Object emptyGroupValue();

	public abstract Node constructOutput(Object groupInfo);
}
