/* $Id: Constant.java,v 1.2 2003/12/24 02:05:48 vpapad Exp $ */
package niagara.logical.path;

public class Constant implements RE {
    String label;

    public Constant(String label) {
        this.label = label;
    }

    public void attachToNFA(NFA nfa) {
        State s = nfa.addState(false);
        nfa.addTransition(s, label, nfa.startState);
        nfa.setStartState(s);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Constant))
            return false;
        if (other.getClass() != Constant.class)
            return other.equals(this);
        Constant constant = (Constant) other;
        return label.equals(constant.label);
    }

    public int hashCode() {
        return label.hashCode();
    }

    public boolean generatesDuplicates() {
        return false;
    }
}