package niagara.logical;

import niagara.optimizer.colombia.Domain;

/**
 * A domain representing Integers
 */
public class IntDomain extends Domain {
	private static IntDomain intDomain;

	static {
		intDomain = new IntDomain();
	}

	public static IntDomain getDomain() {
		return intDomain;
	}

	// private constructor, IntDomain is a singleton
	private IntDomain() {
		super("IntDomain");
	}
}
