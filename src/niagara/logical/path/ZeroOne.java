/* $Id: ZeroOne.java,v 1.1 2003/10/01 04:42:22 vpapad Exp $ */
package niagara.logical.path;

public class ZeroOne implements RE {
    RE re;

    public ZeroOne(RE re) {
        this.re = re;
    }

    public void attachToNFA(NFA nfa) {
        // Old start state
        State os = nfa.startState;

        // New start state
        State ns = nfa.addState(false);

        // Add epsilon transition ns -> os
        nfa.addTransition(ns, Transition.epsilon, os);

        // Connect sub-expression to old start state
        re.attachToNFA(nfa);

        // Add epsilon transition from ns -> sub-expression start state
        nfa.addTransition(ns, Transition.epsilon, nfa.startState);

        // Start state is still ns
        nfa.setStartState(ns);
    }
    
    public boolean equals(Object other) {
        if (other == null || !(other instanceof ZeroOne))
            return false;
        if (other.getClass() != ZeroOne.class)
            return other.equals(this);
        ZeroOne zeroOne = (ZeroOne) other;
        return re.equals(zeroOne.re);
    }

    public int hashCode() {
        return re.hashCode() ^ 3;
    }    
}