/* $Id: RE.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

// Regular expressions
public interface RE {
    /** Transform an NFA to accept this regular expression
    // and then whatever it accepted before */
    void attachToNFA(NFA nfa);
}