/* $Id: NFA.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class NFA {
    private int nextStateId;
    State startState;
    private ArrayList states;
    private ArrayList transitions;

    private HashMap closureMemo; // maps state to list of epsilon-reachable states

    public NFA() {
        nextStateId = 0;
        states = new ArrayList();
        transitions = new ArrayList();
        closureMemo = new HashMap();
    }

    public State addState(boolean accepting) {
        State s = new State(nextStateId++, accepting);
        states.add(s);
        return s;
    }

    public void addTransition(State from, String label, State to) {
        transitions.add(new Transition(from, label, to));
    }

    public void setStartState(State state) {
        startState = state;
    }

    public ArrayList epsilonClosure(State state) {
        // Have we computed the epsilon closure for this state
        // before? Look it up in closureMemo
        if (closureMemo.containsKey(state))
            return (ArrayList) closureMemo.get(state);

        HashMap statesToCheck = new HashMap();
        ArrayList results = new ArrayList();
        results.add(state);
        int count = 0;

        boolean changed = true;

        while (changed) {
            changed = false;

            for (int j = count; j < results.size(); j++)
                statesToCheck.put(results.get(j), Boolean.FALSE);

            count = results.size();

            Iterator keys = statesToCheck.keySet().iterator();

            // For each state in the set, add all the nodes reachable
            // from it with epsilon transitions
            while (keys.hasNext()) {
                State k = (State) keys.next();

                // Do nothing if we have already explored this state
                if (statesToCheck.get(k) == Boolean.TRUE)
                    continue;

                for (int i = 0; i < transitions.size(); i++) {
                    Transition t = (Transition) transitions.get(i);
                    if (t.from == k && t.label == Transition.epsilon) {
                        if (statesToCheck.containsKey(t.to))
                            continue;
                        results.add(t.to);
                        changed = true;
                    }
                }

                // The state is explored
                statesToCheck.put(k, Boolean.TRUE);
            }

        }

        closureMemo.put(state, results);

        return results;
    }

    // Subset construction
    public DFA getDFA() {
        // Create an empty DFA with {epsclosure(startState)} as its only state
        DFA dfa = new DFA();
        DFAState start = dfa.addState(epsilonClosure(startState));
        dfa.setStartState(start);

        boolean allVisited = false;
        HashMap destinations = new HashMap();

        while (!allVisited) {
            allVisited = true;

            for (int k = 0; k < dfa.states.size(); k++) {
                DFAState src = (DFAState) dfa.states.get(k);

                if (src.visited)
                    continue;

                allVisited = false;
                destinations.clear();

                for (int i = 0; i < transitions.size(); i++) {
                    Transition t = (Transition) transitions.get(i);

                    // ignore epsilon transitions
                    if (t.label == Transition.epsilon
                        || !src.nfaStates.contains(t.from))
                        continue;
                    if (destinations.get(t.label) == null)
                        destinations.put(t.label, new ArrayList());
                    else if (
                        ((ArrayList) destinations.get(t.label)).contains(t.to))
                        continue;

                    ((ArrayList) destinations.get(t.label)).addAll(
                        epsilonClosure(t.to));
                }

                Iterator labels = destinations.keySet().iterator();
                while (labels.hasNext()) {
                    String label = (String) labels.next();
                    DFAState dst =
                        dfa.addState((ArrayList) destinations.get(label));
                    src.addTransition(label, dst);
                }

                destinations.clear();
                src.visited = true;
            }
        }

        return dfa;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("NFA has " + states.size());
        sb.append(" states, starting state is " + startState.id);
        sb.append("\n");
        for (int i = 0; i < states.size(); i++) {
            State state = (State) states.get(i);

            sb.append("State " + state.id);
            if (state.accepting)
                sb.append(" (accepting)");
            sb.append("\n");

            sb.append(
                "Epsilon closure = "
                    + arrayListToString(epsilonClosure(state))
                    + "\n");

            for (int j = 0; j < transitions.size(); j++) {
                Transition t = (Transition) transitions.get(j);
                if (t.from == state)
                    sb.append("\t on " + t.label + " -> " + t.to.id + "\n");
            }
        }

        return sb.toString();
    }

    static String arrayListToString(ArrayList ec) {
        StringBuffer sb = new StringBuffer("{");
        for (int j = 0; j < ec.size(); j++) {
            sb.append("" + ((State) ec.get(j)).id + ", ");
        }
        sb.setLength(sb.length() - 2); // Remove the final ", "
        sb.append("}");
        return sb.toString();
    }

}