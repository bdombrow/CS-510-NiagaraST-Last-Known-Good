/* $Id: Epsilon.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

public class Epsilon implements RE {
    public void attachToNFA(NFA nfa) {
        ;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Epsilon))
            return false;
        if (other.getClass() != Epsilon.class)
            return other.equals(this);
        return true;
    }

    public int hashCode() {
        return 0;
    }    
}