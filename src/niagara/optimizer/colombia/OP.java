/* $Id: OP.java,v 1.10 2003/02/08 02:12:03 vpapad Exp $ */
package niagara.optimizer.colombia;

/** Abstract superclass for all operators, logical or physical */
public abstract class Op {
    
    /** Create a copy of this operator. <em>Not</em> (always) the same 
     * as clone() - updatable fields of an operator should be deep cloned.
     * On the other hand it's OK to share pointers to immutable objects,
     * like strings or predicates.
     */
    public abstract Op copy();

    public abstract String getName(); 
    
    /** number of inputs */
    public abstract int getArity();

    /* We require logical and physical operators to redefine 
     * equals and hashCode, in the context of the optimizer.
     * We don't  worry too much about data structures that
     * are used afterwards in query execution.
     * 
     * If there is any way that we can produce a duplicate of an operator
     * using optimizer transformations (e.g. in joins), equals()
     * MUST not be plain pointer equality. The amount of things
     * we must check in equals() depends (as in copy()) on what
     * the optimizer transformations can do. 
     * x.equals(x) and x.copy().equals(x) should always be true.
     * 
     * If we change equals() we must also change hashCode() so 
     * that x.equals(y) => x.hashCode() == y.hashCode() remains true.
     */
    public abstract boolean equals(Object other);
    public abstract int hashCode();

    /** This is a utility method that subclasses can use
     * to compute a hashcode for fields that are allowed to be null */
    protected int hashCodeNullsAllowed(Object o) {
        if (o == null) return 0;
        else return o.hashCode();
    }

    /** This is a utility method that subclasses can use
     * to compute equality for fields that are allowed to be null */
    protected boolean equalsNullsAllowed(Object o1, Object o2) {
        if (o1 != o2) {
            if (o1 == null)
                return o2.equals(o1);
            else 
                return o1.equals(o2);
        }
        return true;
    }
    
    public int getNumberOfOutputs() { return 1;}
    
    public boolean is_logical() {return false;}
    public boolean is_physical() {return false;}
    public boolean is_leaf() {return false;}

    // XXX vpapad: Is anybody actually using this?
    public boolean is_item() { return false; }

    /**
     * Is operator <code>other</code> a valid match for this operator 
     * (in the context of a pattern?)
     * @return boolean
     */
    public boolean matches(Op other) {
            return (getArity() == other.getArity() && getClass() == other.getClass());
    }
}

