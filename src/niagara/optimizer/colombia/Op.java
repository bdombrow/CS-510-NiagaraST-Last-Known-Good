package niagara.optimizer.colombia;

/** Abstract superclass for all operators, logical or physical */
public abstract class Op {
    
    /** Create a copy of this operator. <em>Not</em> (always) the same 
     * as clone() - updateable fields of an operator such as 
     * predicates attached to <code>join</code>s should be deep cloned.
     */
//    public abstract Op copy();

    public abstract String getName(); 
    
    /** number of inputs */
    public abstract int getArity();

//    public abstract boolean equals(Object other);
//    public abstract int hashCode();

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
            return (getClass() == other.getClass());
    }
}
