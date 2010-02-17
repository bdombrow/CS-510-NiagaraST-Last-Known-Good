package niagara.logical;

/**
 * This class is used to represent a punctuation specification.
 * 
 */

public class PunctSpec {

	public enum PunctType {
		ONCHANGE, SOMETHINGELSE
	};

	private PunctType pType;

	public PunctSpec(String punctSpec) {
		if (punctSpec.equalsIgnoreCase("ONCHANGE"))
			;
		this.pType = PunctType.ONCHANGE;
	}

	public PunctType getPunctType() {
		return pType;
	}

	public String toString() {
		switch (pType) {
		case ONCHANGE:
			return "On Change";
		default:
			assert false : "Invalid punctuation type";
			return null;
		}
	}

	public int hashCode() {
		return pType.hashCode();
	}
}
