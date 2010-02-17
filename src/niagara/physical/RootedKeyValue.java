package niagara.physical;

import java.util.ArrayList;

@SuppressWarnings("unchecked")
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

	/**
	 * appendLocalKey appends a local key onto the parent key
	 * 
	 * @param localKey
	 *            the local key to be appended
	 */
	public void appendLocalKeyValueAndTag(ArrayList localKeyVal) {
		rootedKeyVal.add(localKeyVal); /* append local key to end of list */
	}

	/**
	 * determines if two RootedKeyValues are equal
	 * 
	 * @param other
	 *            the RootedKeyValue to be compared with
	 * @return true if equal, false otherwise
	 */
	public boolean equals(RootedKeyValue other) {
		return this.rootedKeyVal.equals(other.rootedKeyVal);
	}

	/**
	 * clones the RootedKeyValue
	 * 
	 * @return Returns a clone of this object
	 */
	public Object clone() {
		RootedKeyValue clonedKey = new RootedKeyValue();
		int len = rootedKeyVal.size();
		for (int i = 0; i < len; i++) {
			clonedKey.rootedKeyVal.add(rootedKeyVal.get(i));
		}
		return clonedKey;
	}

}
