/* $Id: DFA.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

import java.util.ArrayList;

public class DFA {
    ArrayList states;
    DFAState startState;
    int nextStateId;

    public DFA() {
        states = new ArrayList();
        nextStateId = 0;
    }

    public void setStartState(DFAState startState) {
        this.startState = startState;
    }

    public String toString() {
        StringBuffer sb =
            new StringBuffer(
                "DFA has "
                    + states.size()
                    + " states, starting state is "
                    + startState.id
                    + "\n");
        for (int i = 0; i < states.size(); i++) {
            sb.append(states.get(i).toString());
        }
        return sb.toString();
    }

    DFAState addState(ArrayList nfaStates) {
        // Check if we already have this state
        for (int i = 0; i < states.size(); i++) {
            DFAState s = (DFAState) states.get(i);

            if (nfaStates.size() != s.nfaStates.size())
                continue;

            int j;
            for (j = 0; j < s.nfaStates.size(); j++) {
                if (!nfaStates.contains(s.nfaStates.get(j)))
                    break;
            }

            if (j == s.nfaStates.size())
                return s;
        }

        // If any of the NFA states was accepting, this
        // DFA state is also accepting
        boolean accepting = false;
        for (int i = 0; i < nfaStates.size(); i++)
            if (((State) nfaStates.get(i)).accepting) {
                accepting = true;
                break;
            }

        DFAState s = new DFAState(nextStateId++, accepting, nfaStates);
        states.add(s);
        return s;
    }

    public DFAState getStartState() {
        return startState;
    }

    public ArrayList getStates() {
        return states;
    }
}