package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> NoContentNodeMerge </code> element will do nothing.
 * just a placeholder
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.io.PrintStream;

import niagara.utils.ShutdownException;

import org.w3c.dom.Node;

class NoContentNodeMerge extends NodeMerge {

    NoContentNodeMerge() {
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

	return false;
    }

    public void dump(PrintStream os) {
	os.println("No Content Merge");
    }

    public String toString() {
	return "No Content Merge";
    }

    public String getName() {
	return "No Content";
    }
}

