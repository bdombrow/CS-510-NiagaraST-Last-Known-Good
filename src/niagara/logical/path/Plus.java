/* $Id: Plus.java,v 1.2 2003/12/24 02:05:48 vpapad Exp $ */
package niagara.logical.path;

public class Plus implements RE {
    RE re;

    public Plus(RE re) {
        this.re = re;
    }

    public void attachToNFA(NFA nfa) {
        // Old start state
        State os = nfa.startState;

        // Add sub-expression
        re.attachToNFA(nfa);

        // Add epsilon transition from os -> sub-expression start state
        nfa.addTransition(os, Transition.epsilon, nfa.startState);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Plus))
            return false;
        if (other.getClass() != Plus.class)
            return other.equals(this);
        Plus plus = (Plus) other;
        return re.equals(plus.re);
    }

    public int hashCode() {
        return re.hashCode() ^ 1;
    }    
    
    public boolean generatesDuplicates() {
        return true;
    }
}