/* $Id: Bar.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

public class Bar implements RE {
    private RE left, right;

    public Bar(RE left, RE right) {
        this.left = left;
        this.right = right;
    }

    public void attachToNFA(NFA nfa) {
        // Old start state
        State os = nfa.startState;

        // Attach left expression to NFA
        left.attachToNFA(nfa);
        State lstart = nfa.startState;
        // Restore old start state
        nfa.setStartState(os);

        // Attach right expression to NFA
        right.attachToNFA(nfa);
        State rstart = nfa.startState;

        // Create a new start state
        State ns = nfa.addState(false);

        // Add epsilon transitions from ns
        nfa.addTransition(ns, Transition.epsilon, lstart);
        nfa.addTransition(ns, Transition.epsilon, rstart);

        // Start state is ns
        nfa.setStartState(ns);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Bar))
            return false;
        if (other.getClass() != Bar.class)
            return other.equals(this);
        Bar bar = (Bar) other;
        return left.equals(bar.left) && right.equals(bar.right);
    }

    public int hashCode() {
        return left.hashCode() ^ right.hashCode();
    }
}