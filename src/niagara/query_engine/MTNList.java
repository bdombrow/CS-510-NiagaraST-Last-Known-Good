/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

class MTNList {
    static private int TABLE_SIZE = 30;
    private MergeTreeNode head;
    //private HashMap hashFragName;
    //private MergeTreeNode[] hashTable;

    public MTNList() {
	head = null;
	//hashFragName = new HashMap(); // default init capacity is 16
	//hashTable = new MergeTreeNode[TABLE_SIZE];
    }

    public void add(MergeTreeNode newNode) {
	//put(newNode.getFragTagName(), newNode);
	newNode.next = head;
	head = newNode;
	//Object o = hashFragName.put(newNode.getFragTagName(), newNode);
	//assert o == null : "duplicate entry in MTNList";
    }

    //public MergeTreeNode getHead() {
    //	return head;
    //}

    public MergeTreeNode getItemWithFragTagName(String fragName) {
	//return get(fragName);
	//return (MergeTreeNode)hashFragName.get(fragName);
	    
	MergeTreeNode child = head;

	while(child != null && !child.getFragTagName().equals(fragName)) {
	    child = child.next;
	}
	return child;
    }
    /*
    private void put(String fragName, MergeTreeNode entry) {
	int hashVal = hashCode(fragName);
	assert hashVal < hashTable.length && hashVal >= 0
	    : "hashVal " + hashVal + " len "
	    + hashTable.length;
	entry.next = hashTable[hashVal];
	hashTable[hashVal] = entry;
    }

    private MergeTreeNode get(String fragName) {
	int hashVal = hashCode(fragName);

	MergeTreeNode curr = hashTable[hashVal];
	while(curr != null) {
	    if(curr.getFragTagName().equals(fragName))
		return curr;
	    curr = curr.next;
	}
	return null;
    }

    private int hashCode(String fragName) {
        int hc = fragName.hashCode() % TABLE_SIZE;
	if(hc < 0)
	    return 0-hc;
	else
	    return hc;
    }
    */
}

