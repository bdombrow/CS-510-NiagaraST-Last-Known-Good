package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> ExactMatchNodeMerge </code> element will verify
 * that two nodes match.
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.lang.*;
import java.lang.reflect.*;
import java.io.*;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.utils.type_system.*;

class ExactMatchNodeMerge extends NodeMerge {

    private NodeHelper comparator;

    ExactMatchNodeMerge(NodeHelper _comparator) {
	/* just do anything - it doesn't matter */
	setInnerOuter(MergeTree.MT_OUTER);
	comparator = _comparator;
	name = null;
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
    boolean merge(Node lNode, Node rNode, Node resultNode) 
	throws ShutdownException {

	if(lNode == null) {
	    if(rNode == null) {
		return false; // OK
	    } else {
		DOMHelper.setTextValue(resultNode,
				       DOMHelper.getTextValue(rNode));
		return false;
	    }
	}
	

	if(!comparator.nodeEquals(lNode, rNode)) {
	    throw new 
		ShutdownException("Non-matching elements in ExactMatchMerge. lNode: " 
		   + lNode.getNodeName() + "(" + DOMHelper.getTextValue(lNode) +")" +
				   "  rNode: "
		   + rNode.getNodeName() + "(" + DOMHelper.getTextValue(rNode) +")");
	} else {
	    /* everything is OK */
	    return false;
	}
    }

    public void dump(PrintStream os) {
	os.println("Exact Match Merge:");
	os.println("Comparator: " + comparator.getName());
    }

    public String toString() {
	return "ExactMatch Merge " + comparator.getName();
    }

    public String getName() {
	return "ExactMatch";
    }

}

