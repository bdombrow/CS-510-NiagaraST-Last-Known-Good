/* $Id: Epsilon.java,v 1.2 2003/12/24 02:05:48 vpapad Exp $ */
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
    
    public boolean generatesDuplicates() {
        return true;
    }    
}