/**********************************************************************
  $Id: PathExprEvaluator.java,v 1.14 2003/02/25 06:10:25 vpapad Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


package niagara.query_engine;

import org.w3c.dom.*;
import java.util.*;

import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.*;

public class PathExprEvaluator {
    private DFA dfa;
    private NodeVector nodes;
    private ArrayList states;
    private ArrayList prevStates;
    private int top;

    private HashSet matches;

    public PathExprEvaluator(regExp r) {
        RE re = regExp2RE(r);

        NFA nfa = new NFA();

        // Add an accepting state
        State start = nfa.addState(true);
        nfa.setStartState(start);
        
        // Attach the regular expression
        re.attachToNFA(nfa);
        
        // Create the DFA
        dfa = nfa.getDFA();

        nodes = new NodeVector();
        states = new ArrayList();
        prevStates = new ArrayList();
        matches = new HashSet();
    }

    public void getMatches(Node n, NodeVector results) {
        DFAState s = dfa.startState;
        DFAState ps = null;

        top = -1;

        // XXX vpapad not handling attributes yet

        while (top >= 0 || n != null) {
            if (n == null) {
                n = nodes.get(top);
                s = (DFAState) states.get(top);
                ps = (DFAState) prevStates.get(top);
                top--;
            }

            // If the state is accepting, add node to results,
            // if it's not already there
            if (s.accepting && matches.add(n))
                    results.add(n);


	    // Put any attribute that matches a transition from
            // the current state to an accepting state in the results
	    // If the transition leads to a non-accepting state,
	    // we know that it's useless to follow it on attributes,
	    // since they don't have any descendants.

	    if (n.getNodeType() == Node.ELEMENT_NODE) {
		HashMap transitions = s.transitions;
		DFAState onWildcard = s.onWildcard;

		NamedNodeMap nnm = n.getAttributes();
		int nattrs = nnm.getLength();
		for (int i = 0; i < nattrs; i++) {
		    Attr a = (Attr) nnm.item(i);
		    DFAState next = (DFAState) transitions.get(a.getName());
		    if (next != null && next.accepting && matches.add(a))
			results.add(a);
		    if (onWildcard != null && onWildcard != next
			&& onWildcard.accepting && matches.add(a))
			results.add(a);
		}
	    }

            Node currentNode = n;
            DFAState currentState = s;

	    
            // If there's only one 'next node', we'll skip the stack
            // and store it in n
            n = null;

            // Find this node's next sibling that matches a transition
            // from the previous state
            if (ps != null) {
                Node sibling = currentNode.getNextSibling();

                HashMap transitions = ps.transitions;
                DFAState onWildcard = ps.onWildcard;

                boolean foundOne = false;                
                while (sibling != null) {		    
                    // Ignore text nodes for path following
                    if (sibling.getNodeType() != Node.TEXT_NODE) {
                        String label = sibling.getNodeName();
                        DFAState next = (DFAState) transitions.get(label);
                        if (onWildcard != null && onWildcard != next) {
                            foundOne = true;
                            // At this point, n is guaranteed to be null
                            n = sibling;
                            s = onWildcard;
                            // Previous state is still ps
                        }
                        if (next != null) {
                            foundOne = true;
                            // If we found a 'next state' in the previous
                            // "if", we're now forced to push it on the stack
                            if (n != null) {
                                top++;
                                nodes.add(top, n);
                                states.add(top, s);
                                prevStates.add(top, ps);
                            }
                            n = sibling;
                            s = next;
                        }
                    }
                    // Only have to find the next matching sibling
                    if (foundOne) break;
                    sibling = sibling.getNextSibling();
                }
            }

            Node c = currentNode.getFirstChild();
            HashMap transitions = currentState.transitions;
            DFAState onWildcard = currentState.onWildcard;

            boolean foundOne = false;
            while (c != null) {
                // Ignore text nodes for path following
                if (c.getNodeType() != Node.TEXT_NODE) {
                    String label = c.getNodeName();
                    DFAState next = (DFAState) transitions.get(label);
                    if (onWildcard != null && onWildcard != next) {
                        foundOne = true;
                        // If we found a 'next state' in the previous
                        // "ifs", we're now forced to push it on the stack
                        if (n != null) {
                            top++;
                            nodes.add(top, n);
                            states.add(top, s);
                            prevStates.add(top, ps);
                        } 
                        n = c;
                        s = onWildcard;
                        // Current state becomes our children's previous state
                        ps = currentState; 
                    }
                    if (next != null) {
                        foundOne = true;
                        if (n != null) {
                            top++;
                            nodes.add(top, n);
                            states.add(top, s);
                            prevStates.add(top, ps);
                        }
                        n = c;
                        s = next;
                        // Current state becomes our children's previous state
                        ps = currentState;
                    }
                }
                if (foundOne) break;
                c = c.getNextSibling();
            }
        }

        // Clean up stacks
        nodes.clear();
        states.clear();
        prevStates.clear();
        matches.clear();
    }

    private static RE regExp2RE(regExp r) {
        if (r == null)
            return new Epsilon();

        if (r instanceof regExpDataNode) 
            return new Constant(((String) ((regExpDataNode) r).getData().getValue()));

        regExpOpNode rop = (regExpOpNode) r;
        switch (rop.getOperator()) {
        case opType.BAR:
            return new Bar(regExp2RE(rop.getLeftChild()), 
                           regExp2RE(rop.getRightChild()));
        case opType.DOT:
            return new Dot(regExp2RE(rop.getLeftChild()), 
                           regExp2RE(rop.getRightChild()));
        case opType.DOLLAR:
            return new Wildcard();
        case opType.QMARK:
            return new ZeroOne(regExp2RE(rop.getLeftChild()));
        case opType.STAR:
            return new Star(regExp2RE(rop.getLeftChild()));
        case opType.PLUS:
            return new Plus(regExp2RE(rop.getLeftChild()));
        default: 
            throw new PEException("Unknown operator in regExp2RE");
        }
    }
}

class NFA {
    int nextStateId;
    State startState;
    ArrayList states;
    ArrayList transitions;

    HashMap closureMemo; // maps state to list of epsilon-reachable states

    NFA() {
        nextStateId = 0;
        states = new ArrayList();
        transitions = new ArrayList();
        closureMemo = new HashMap();
    }

    State addState(boolean accepting) {
        State s = new State(nextStateId++, accepting);
        states.add(s);
        return s;
    }

    void addTransition(State from, String label, State to) {
        transitions.add(new Transition(from, label, to));
    }

    void setStartState(State state) {
        startState = state;
    }

    ArrayList epsilonClosure(State state) {
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
                    else if (((ArrayList) destinations.get(t.label))
                             .contains(t.to))
                        continue;
                    
                    ((ArrayList) destinations.get(t.label))
                            .addAll(epsilonClosure(t.to));
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
            
            sb.append("Epsilon closure = " 
                      + arrayListToString(epsilonClosure(state)) + "\n");

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

class DFAState extends State {
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
}

class DFA {
    ArrayList states;
    DFAState startState;
    int nextStateId;

    DFA() {
        states = new ArrayList();
        nextStateId = 0;
    }

    void setStartState(DFAState startState) {
        this.startState = startState;
    }

    public String toString() {
        StringBuffer sb = 
            new StringBuffer("DFA has " + states.size() 
                             + " states, starting state is " 
                             + startState.id + "\n");
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
}

// Regular expressions

abstract class RE {
    // Transform an NFA to accept this regular expression
    // and then whatever it accepted before
    abstract void attachToNFA(NFA nfa);
}

class Epsilon extends RE {
    void attachToNFA(NFA nfa) {
        ;
    }
}

class Constant extends RE {
    String label;
    
    Constant(String label) {
        this.label = label;
    }

    void attachToNFA(NFA nfa) {
        State s = nfa.addState(false);
        nfa.addTransition(s, label, nfa.startState);
        nfa.setStartState(s);
    }
}

class Wildcard extends RE {

    void attachToNFA(NFA nfa) {
        State s = nfa.addState(false);
        nfa.addTransition(s, Transition.wildcard, nfa.startState);
        nfa.setStartState(s);
    }
}

class Dot extends RE {
    RE left, right;

    Dot(RE left, RE right) {
        this.left = left;
        this.right = right;
    }
    
    void attachToNFA(NFA nfa) {
        // attach in reverse order
        right.attachToNFA(nfa);
        left.attachToNFA(nfa);
    }
}

class Bar extends RE {
    RE left, right;

    Bar (RE left, RE right) {
        this.left = left;
        this.right = right;
    }

    void attachToNFA(NFA nfa) {
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
}

class Star extends RE {
    RE re;

    Star(RE re) {
        this.re = re;
    }

    void attachToNFA(NFA nfa) {
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
}

class Plus extends RE {
    RE re;

    Plus(RE re) {
        this.re = re;
    }

    void attachToNFA(NFA nfa) {
        // Old start state
        State os = nfa.startState;

        // Add sub-expression
        re.attachToNFA(nfa);

        // Add epsilon transition from os -> sub-expression start state
        nfa.addTransition(os, Transition.epsilon, nfa.startState);
    }
}

class ZeroOne extends RE{
    RE re;
    
    ZeroOne(RE re) {
        this.re = re;
    }

    void attachToNFA(NFA nfa) {
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
}

class Transition {
    State from;
    String label;
    State to;
    
    Transition(State from, String label, State to) {
        this.from = from;
        this.label = label;
        this.to = to;
    }

    // special constants
    static String epsilon = new String("epsilon");
    static String wildcard = new String("*");
}


class State {
    int id;
    boolean accepting;

    State(int id, boolean accepting) {
        this.id = id;
        this.accepting = accepting;
    }
}

