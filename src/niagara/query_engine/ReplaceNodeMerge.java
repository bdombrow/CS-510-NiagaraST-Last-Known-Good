package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> ReplaceNodeMerge </code> element will merge two nodes
 * together. The value from the dominant side will be considered
 * the value for the result. This is a type of shallow content merge,
 * this is not deep replace
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.lang.*;
import java.lang.reflect.*;
import java.io.*;

import niagara.utils.*;
import niagara.utils.type_system.*;
import niagara.utils.nitree.*;

class ReplaceNodeMerge extends NodeMerge {

    /* isDominant indicates which side is dominant if
     * content or attribute values are equal
     * valid for dominant content and exact match merges
     */
    private boolean leftIsDominant;

    private NodeHelper comparator;

    ReplaceNodeMerge(int domSide, int mergeType,
		     NodeHelper _comparator) {
	/* set whether leftIsDominant */
	switch(domSide) {
	case MergeTree.DS_LEFT:
	    leftIsDominant = true;
	    break;
	case MergeTree.DS_RIGHT:
	    leftIsDominant = false;
	    break;
	default:
	    throw new PEException("Invalid dominant side");
	}

	/* set up for the merge type - inner,outer, etc */
	setInnerOuter(mergeType);

	comparator = _comparator;
	name = null;

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

    boolean merge(NINode lNode, NINode rNode, NINode resultNode) 
	throws OpExecException, NITreeException {

	NINode dominantNode = null;
	NINode submissiveNode = null;
	if(leftIsDominant) {
	    dominantNode = lNode;
	    submissiveNode = rNode;
	} else {
	    dominantNode = rNode;
	    submissiveNode = lNode;
	}
	
	if(dominantNode != resultNode) {
	    /* set the value of resultNode only if we absolutly
	     * have to
	     */
	    if(!comparator.nodeEquals(dominantNode, resultNode)) { 
		resultNode.mySetNodeValue(dominantNode.myGetNodeValue());
	    }
	    return true;
	} else { 
	    /* else nothing to do - result is already dominant or
	       dominant and submissive values are the same */
	    return false;
	}
    }    

    public void dump(PrintStream os) {
	os.print("Replace Node Merge:");
	if(leftIsDominant == true) {
	    os.print(" keep left ");
	} else {
	    os.print(" keep right ");
	}
	os.print("Comparator: " + comparator.getName());
	os.println();
    }

    public String toString() {
	return "Replace Node Merge " + comparator.getName();
    }

    public String getName() {
	return "ReplaceNode";
    }
}
