package niagara.logical.path;

public class Star implements RE {
	RE re;

	public Star(RE re) {
		this.re = re;
	}

	public void attachToNFA(NFA nfa) {
		// Old start state
		State os = nfa.startState;

		// New start state
		State ns = nfa.addState(false);
		nfa.setStartState(ns);

		// Add epsilon transition ns -> os
		nfa.addTransition(ns, Transition.epsilon, os);

		// Add sub-expression
		re.attachToNFA(nfa);

		// Add epsilon transition from ns -> sub-expression start state
		nfa.addTransition(ns, Transition.epsilon, nfa.startState);

		// Start state is still ns
		nfa.setStartState(ns);
	}

	public boolean equals(Object other) {
		if (other == null || !(other instanceof Star))
			return false;
		if (other.getClass() != Star.class)
			return other.equals(this);
		Star star = (Star) other;
		return re.equals(star.re);
	}

	public int hashCode() {
		return re.hashCode() ^ 2;
	}

	public boolean generatesDuplicates() {
		return true;
	}
}