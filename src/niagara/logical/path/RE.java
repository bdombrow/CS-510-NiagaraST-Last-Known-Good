package niagara.logical.path;

// Regular expressions
public interface RE {
	/**
	 * Transform an NFA to accept this regular expression and then whatever it
	 * accepted before
	 */
	void attachToNFA(NFA nfa);

	/**
	 * Is it possible that two nodes may be linked using this regular expression
	 * along two different paths?
	 */
	boolean generatesDuplicates();
}