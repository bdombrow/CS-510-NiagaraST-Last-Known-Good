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

import java.io.PrintStream;

import niagara.utils.DOMHelper;
import niagara.utils.ShutdownException;
import niagara.utils.type_system.NodeHelper;

import org.w3c.dom.Node;

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

	if(lNode == null && rNode == null) {
	    return false; // OK
	} 

	if(lNode == null) {
	    // know node is not null
	    if(resultNode != rNode) {
		DOMHelper.setTextValue(resultNode,
				       DOMHelper.getTextValue(rNode));
		return true;
	    }
	    return false;
	}
       
	// neither lNode or rNode is null
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

