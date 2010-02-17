package niagara.logical;

/**
 * This class is used to represent a prefetch specification.
 * 
 */

public class PrefetchSpec {

	// prefetch and coverage are specified in seconds;

	private int prefetchVal;
	private int coverage;

	public void setCoverage(int _coverage) {
		coverage = _coverage;
	}

	public int getCoverage() {
		return coverage;
	}

	public void setPrefetchVal(int val) {
		prefetchVal = val;
	}

	public int getPrefetchVal() {
		return prefetchVal;
	}

	public enum PrefetchType {
		SOMETHING, SOMETHINGELSE
	};

	private PrefetchType pfType = PrefetchType.SOMETHING;

	public PrefetchSpec(String pfSpecStr) {
		String[] parameters = pfSpecStr.split("[\t| ]+");
		prefetchVal = Integer.valueOf(parameters[0]);
		coverage = Integer.valueOf(parameters[1]);
	}

	public PrefetchType getPrefetchType() {
		return pfType;
	}

	public String toString() {
		switch (pfType) {
		case SOMETHING:
			return "something";
		default:
			assert false : "Invalid prefetch type";
			return null;
		}
	}

	public int hashCode() {
		return pfType.hashCode();
	}

}
