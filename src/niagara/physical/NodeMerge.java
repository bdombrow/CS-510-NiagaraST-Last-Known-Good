package niagara.physical;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> NodeMerge </code> element will merge two nodes
 * together. Nodes may be attributes or elements - would be
 * nice if the aggregation code could handle both element
 * and attr nodes, for example
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.io.PrintStream;

import niagara.utils.ShutdownException;

import org.w3c.dom.Node;

/** Base class for three types of node merges - 
 * 1) take the dominant content
 * 2) do some aggregation of the content 
 * 3) exact match
 */
abstract class NodeMerge {

    /* includeOuter is used to
     * specify what is to be done when an attr from left,right
     * is found not in the other side - together - they can
     * be used to specify the full join spectrum, inner, outer,
     * left outer, right outer.  Note only inner and right outer
     * make sense in the accumulate case.
     * These values not used in merge, just stored here 
     * must be set for dominantcontent and aggregate merges
     */
    protected boolean includeLeftOuter;
    protected boolean includeRightOuter;
    
    protected String name; /* mainly for debugging purposes */
    
    protected void setInnerOuter(int mergeType) {
	switch(mergeType) {
	case MergeTree.MT_OUTER:
	    includeLeftOuter = true;
	    includeRightOuter = true;
	    break;
	case MergeTree.MT_LEFTOUTER:
	    includeLeftOuter = true;
	    includeRightOuter = false;
	    break;
	case MergeTree.MT_RIGHTOUTER:
	    includeLeftOuter = false;
	    includeRightOuter = true;
	    break;
	case MergeTree.MT_INNER:
	    includeLeftOuter = false;
	    includeRightOuter = false;
	    break;
	default:
	    assert false : "KT: Invalid Merge Type";
	}
	return;
    }

    /**
     * Function to merge two nodes (can be attrs or elements)
     *
     * @param lNode left node to be merged
     * @param rNode right node to be merged
     * @param resultNode - the place to put the new result
     * 
     * @return Returns true if resultNode changed, false if
     *         no updates need to be made based on this merge
     */
    abstract boolean merge(Node lNode, Node rNode, Node resultNode)
	throws ShutdownException;

    /**
     * Function to set name for this node merge object
     * no real use for this other than debugging - will typically
     * be the attr name if set
     *
     * @param _name Name to be used for this <code> NodeMerge </code> object.
     */
    void setName(String _name) {
	name = _name;
    }

    /**
     * Indicates if tuples from the "left" side, with no matching
     * tuples on the "right" side should be included in result
     *
     */
    boolean includeLeftOuter() {
	return includeLeftOuter;
    }

    /**
     * Indicates if tuples from the "right" side, with no matching
     * tuples on the "left" side should be included in result
     *
     */
    boolean includeRightOuter() {
	return includeRightOuter;
    }

    public abstract void dump(PrintStream os);
    public abstract String getName();
}

