/* $Id: RE.java,v 1.2 2003/12/24 02:05:48 vpapad Exp $ */
package niagara.logical.path;

// Regular expressions
public interface RE {
    /** Transform an NFA to accept this regular expression
           and then whatever it accepted before */
    void attachToNFA(NFA nfa);
    
    /** Is it possible that two nodes may be linked using
     * this regular expression along two different paths? */
    boolean generatesDuplicates();
}