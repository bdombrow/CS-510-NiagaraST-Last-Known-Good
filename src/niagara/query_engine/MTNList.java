/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/** <code> MTNList </code> a simple linked list of Merge Tree Nodes
 * implemented to avoid allocation in ArrayList.getIterator
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */


class MTNList {
    private MergeTreeNode head;

    public MTNList() {
	head = null;
    }

    public void add(MergeTreeNode newNode) {
	newNode.next = head;
	head = newNode;
    }

    public MergeTreeNode getHead() {
	return head;
    }

}
