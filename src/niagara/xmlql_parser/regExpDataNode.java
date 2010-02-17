package niagara.xmlql_parser;

/**
 * 
 * This class is used to represent the leaf node (that contains string,
 * variables, etc.)
 * 
 * 
 */

public class regExpDataNode extends regExp {

	private data expData; // leaf data (could be string, variable, etc.)

	/**
	 * Constructor
	 * 
	 * @param data
	 *            that represents the leaf of regular expression tree
	 */

	public regExpDataNode(data d) {
		expData = d;
	}

	/**
	 * @return the leaf data
	 */

	public data getData() {
		return expData;
	}

	/**
	 * print the data to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		expData.dump(depth);
	}

	// trigger --Jianjun
	public String toString() {
		return expData.toString();
	}

	public boolean isNever() {
		if (expData.getType() == dataType.VAR) {
			if (((String) (expData.getValue())).equals("NEVER")) {
				return true;
			}
		}
		return false;
	}
}
