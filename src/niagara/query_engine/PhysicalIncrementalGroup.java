/* $Id: PhysicalIncrementalGroup.java,v 1.2 2002/10/24 03:17:09 vpapad Exp $ */
package niagara.query_engine;

import java.util.HashMap;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Collection;
import java.util.Iterator;

import niagara.logical.IncrementalGroup;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

import org.w3c.dom.*;

/** Implementation of IncrementalGroup */
public abstract class PhysicalIncrementalGroup extends PhysicalOperator {
    // No blocking input streams
    private static final boolean[] blockingSourceStreams = { false };

    // The logical operator for grouping
    protected IncrementalGroup logicalGroupOperator;

    // The list of attributes to group by
    private Vector groupAttrs;

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

    public void initFrom(LogicalOp logicalOperator) {
        // Typecast to a group logical operator
        logicalGroupOperator = (IncrementalGroup) logicalOperator;
    }

    /**
     * This function initializes the data structures for an operator.
     * This over-rides the corresponding function in the base class.
     *
     * @return True if the operator is to continue and false otherwise
     */
    protected void opInitialize() {
        // Get the grouping attributes
        skolem grouping = logicalGroupOperator.getSkolemAttributes();

        groupAttrs = grouping.getVarList();

        hasher = new Hasher(groupAttrs);

        // Initialize the hash tables
        hash2final = new HashMap();
        hash2partial = new HashMap();
	hash2tuple = new HashMap();
    }

    /**
     * @see niagara.query_engine.PhysicalOperator#nonblockingProcessSourceTupleElement(StreamTupleElement, int)
     */
    protected void nonblockingProcessSourceTupleElement(
        StreamTupleElement tuple,
        int streamId)
        throws ShutdownException, InterruptedException, UserErrorException {
        String hash = hasher.hashKey(tuple);

        // Have we seen this group before?
        StreamTupleElement representativeTuple =
            (StreamTupleElement) hash2tuple.get(hash);
        if (representativeTuple == null) {
            // Output tuples contain the groupby attributes plus
            // two more fields: previous and current group info
            representativeTuple =
                new StreamTupleElement(true, groupAttrs.size() + 2);
            // Copy the groupby attributes
            for (int i = 0; i < groupAttrs.size(); i++) {
                schemaAttribute sa = (schemaAttribute) groupAttrs.get(i);
                representativeTuple.appendAttribute(
                    tuple.getAttribute(sa.getAttrId()));
            }
            // Initialize the group hashtable entries
            hash2partial.put(hash, emptyGroupValue());
            hash2final.put(hash, emptyGroupValue());
            hash2tuple.put(hash, representativeTuple);
        }

        StreamTupleElement newTuple = 
	    new StreamTupleElement(tuple.isPartial(),
				   groupAttrs.size() + 2);
	newTuple.appendAttributes(representativeTuple);

        Node oldResult = null;
	Node newResult;
        // partial tuples only affect partial results
	Object oldGroupInfo = hash2partial.get(hash);
	if (outputOldValue())
	    oldResult = constructOutput(oldGroupInfo);
        Object newGroupInfo = processTuple(tuple, oldGroupInfo);
        newResult = constructOutput(newGroupInfo);
        hash2partial.put(hash, newGroupInfo);
        // final tuples affect both final and partial results
        if (!tuple.isPartial()) {
            // final tuples affect both final and partial results
	    oldGroupInfo = hash2final.get(hash);
	    if (outputOldValue())
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
	if (outputOldValue())
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

    protected boolean outputOldValue() {
	return false;
    }

    public abstract Object processTuple(
        StreamTupleElement tuple,
        Object previousGroupInfo);
    public abstract Object emptyGroupValue();
    public abstract Node constructOutput(Object groupInfo);
}
