/* $Id: Dot.java,v 1.2 2003/12/24 02:05:48 vpapad Exp $  */
package niagara.logical.path;

public class Dot implements RE {
    private RE left, right;

    public Dot(RE left, RE right) {
        this.left = left;
        this.right = right;
    }

    public void attachToNFA(NFA nfa) {
        // attach in reverse order
        right.attachToNFA(nfa);
        left.attachToNFA(nfa);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Dot))
            return false;
        if (other.getClass() != Dot.class)
            return other.equals(this);
        Dot dot = (Dot) other;
        return left.equals(dot.left) && right.equals(dot.right);
    }

    public int hashCode() {
        return left.hashCode() ^ right.hashCode();
    }

    public boolean generatesDuplicates() {
        return left.generatesDuplicates() || right.generatesDuplicates();
    }
}