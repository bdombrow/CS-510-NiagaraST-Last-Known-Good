package niagara.logical.path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

@SuppressWarnings("unchecked")
public class DFAState extends State {
	HashMap transitions;
	DFAState onWildcard;

	// set of NFA states this state corresponds to
	ArrayList nfaStates;
	boolean visited;

	DFAState(int id, boolean accepting, ArrayList nfaStates) {
		super(id, accepting);
		transitions = new HashMap();
		this.nfaStates = nfaStates;

		visited = false;
		onWildcard = null;
	}

	void addTransition(String label, DFAState to) {
		if (label == Transition.wildcard)
			onWildcard = to;
		else
			transitions.put(label, to);
	}

	public DFAState getNext(String label) {
		return (DFAState) transitions.get(label);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer("State " + id);
		if (accepting)
			sb.append(" (accepting)");

		sb.append(" " + NFA.arrayListToString(nfaStates));
		sb.append("\n");

		Iterator labels = transitions.keySet().iterator();
		while (labels.hasNext()) {
			String label = (String) labels.next();
			DFAState dst = (DFAState) transitions.get(label);
			sb.append("\t on " + label + " -> " + dst.id + "\n");
		}

		if (onWildcard != null)
			sb.append("\t on * -> " + onWildcard.id + "\n");
		return sb.toString();
	}

	public ArrayList getNFAStates() {
		return nfaStates;
	}

	public DFAState getOnWildcard() {
		return onWildcard;
	}

	public HashMap getTransitions() {
		return transitions;
	}

	public boolean isVisited() {
		return visited;
	}
}