package niagara.physical;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * Definition of class <code> RootedKeyValue </code>. Simply a class
 * which holds a rooted key value. Used just to protect us from
 * future changes to rooted key value structure. Really, I think
 * an ArrayList would suffice, but this gives us the ability to define
 * some nice assistant functions on RootedKeyValue as well as future
 * flexibility
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.ArrayList;

class RootedKeyValue {

    /* Contains a list of the key value lists */
    ArrayList rootedKeyVal;

    /**
     * Constructor - does nothing 
     */
    public RootedKeyValue() {
	rootedKeyVal = new ArrayList();
    }

    public int hashCode() {
	return rootedKeyVal.hashCode();
    }

    /** appendLocalKey appends a local key onto the parent key
     * @param localKey the local key to be appended
     */
    public void appendLocalKeyValueAndTag(ArrayList localKeyVal) {
	rootedKeyVal.add(localKeyVal); /* append local key to end of list */
    }

    /** determines if two RootedKeyValues are equal
     * @param other the RootedKeyValue to be compared with
     * @return true if equal, false otherwise
     */
    public boolean equals(RootedKeyValue other) {
	return this.rootedKeyVal.equals(other.rootedKeyVal);
    }

    /**
     * clones the RootedKeyValue
     * @return Returns a clone of this object
     */
    public Object clone() {
	RootedKeyValue clonedKey = new RootedKeyValue();
	int len = rootedKeyVal.size();
	for(int i=0; i<len; i++) {
	    clonedKey.rootedKeyVal.add(rootedKeyVal.get(i));
	}
	return clonedKey;
    }

}



