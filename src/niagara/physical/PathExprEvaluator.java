/**********************************************************************
  $Id: PathExprEvaluator.java,v 1.1 2003/12/24 01:49:01 vpapad Exp $


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

package niagara.physical;

import org.w3c.dom.*;
import java.util.*;

import niagara.xmlql_parser.*;
import niagara.logical.path.*;
import niagara.utils.*;

public class PathExprEvaluator {
    private DFA dfa;
    private NodeVector nodes;
    private ArrayList states;
    private ArrayList prevStates;
    private final boolean checkDuplicates;

    private HashSet matches;

    public PathExprEvaluator(regExp r) {
        this(regExp.regExp2RE(r));
    }

    public PathExprEvaluator(RE re) {
        NFA nfa = new NFA();

        // Add an accepting state
        State start = nfa.addState(true);
        nfa.setStartState(start);

        checkDuplicates = re.generatesDuplicates();

        // Attach the regular expression
        re.attachToNFA(nfa);

        // Create the DFA
        dfa = nfa.getDFA();

        nodes = new NodeVector();
        states = new ArrayList();
        prevStates = new ArrayList();
        if (checkDuplicates)
            matches = new HashSet();
    }

    // XXX vpapad: The "new and improved" PathExprEvaluator interface
    // Results are streamed out through callbacks to NodeConsumer.consume
    public void produceMatches(Node n, NodeConsumer results)
        throws ShutdownException, InterruptedException {
        DFAState s = dfa.getStartState();
        DFAState ps = null;

        int top = -1;

        // XXX vpapad not handling attributes yet

        while (top >= 0 || n != null) {
            if (n == null) {
                n = nodes.pop();
                s = (DFAState) states.get(top);
                ps = (DFAState) prevStates.get(top);
                top--;
            }

            // If the state is accepting, add node to results,
            // if it's not already there
            if (s.accepting) 
                outputMatch(n, results);

            // Put any attribute that matches a transition from
            // the current state to an accepting state in the results
            // If the transition leads to a non-accepting state,
            // we know that it's useless to follow it on attributes,
            // since they don't have any descendants.
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                HashMap transitions = s.getTransitions();
                DFAState onWildcard = s.getOnWildcard();

                NamedNodeMap nnm = n.getAttributes();
                int nattrs = nnm.getLength();
                for (int i = 0; i < nattrs; i++) {
                    Attr a = (Attr) nnm.item(i);
                    DFAState next = (DFAState) transitions.get(a.getName());
                    if (next != null && next.accepting)
                        outputMatch(a, results);
                    if (onWildcard != null
                        && onWildcard != next
                        && onWildcard.accepting)
                        outputMatch(a, results);
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

                HashMap transitions = ps.getTransitions();
                DFAState onWildcard = ps.getOnWildcard();

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
                                pushOnStack(top, n, s, ps);
                            }
                            n = sibling;
                            s = next;
                        }
                    }
                    // Only have to find the next matching sibling
                    if (foundOne)
                        break;
                    sibling = sibling.getNextSibling();
                }
            }

            Node c = currentNode.getFirstChild();
            HashMap transitions = currentState.getTransitions();
            DFAState onWildcard = currentState.getOnWildcard();

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
                            pushOnStack(top, n, s, ps);
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
                            pushOnStack(top, n, s, ps);
                        }
                        n = c;
                        s = next;
                        // Current state becomes our children's previous state
                        ps = currentState;
                    }
                }
                if (foundOne)
                    break;
                c = c.getNextSibling();
            }
        }

        // Clean up stacks
        nodes.clear();
        states.clear();
        prevStates.clear();
        if (checkDuplicates)
            matches.clear();
    }

    private void outputMatch(Node n, NodeConsumer nc)
        throws InterruptedException, ShutdownException {
        if (!checkDuplicates || matches.add(n))
            nc.consume(n);
    }

    private void pushOnStack(int top, Node n, DFAState s, DFAState ps) {
        nodes.add(top, n);
        int size = states.size();
        assert top <= size;
        if (top == size) {
            states.add(s);
            prevStates.add(ps);
        } else {
            states.set(top, s);
            prevStates.set(top, ps);
        }
    }

    // XXX vpapad: "PathExprEvaluator classic" interface
    // Results are accumulated in a NodeVector
    public void getMatches(Node n, NodeVector results) {
        try {
            produceMatches(n, results);
        } catch (ShutdownException se) {
            throw new PEException("NodeVector.consume shouldn't throw ShutdownException!");
        } catch (InterruptedException ie) {
            throw new PEException("NodeVector.consume shouldn't throw InterruptedException!");
        }
    }
}