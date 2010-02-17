package niagara.logical;

import niagara.optimizer.colombia.Domain;

/**
 * A domain representing Doubles
 */
public class DoubleDomain extends Domain {
	private static DoubleDomain doubleDomain;

	static {
		doubleDomain = new DoubleDomain();
	}

	public static DoubleDomain getDomain() {
		return doubleDomain;
	}

	// private constructor, IntDomain is a singleton
	private DoubleDomain() {
		super("DoubleDomain");
	}
}
