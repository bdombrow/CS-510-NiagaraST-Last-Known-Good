package niagara.optimizer.colombia;

/**
  *  Leaf operators, used in rules only.  Placeholder for a Group.
  */
public class LEAF_OP extends Op {

    private Group group;
    //Identifies the group bound to this leaf, after binding.
    // == null until binding
    private int index; //Used to distinguish this leaf in a rule

    public LEAF_OP(int index) {
        this(index, null);
    }

    public int getArity() {
        return (0);
    }
    
    public Group getGroup() {
        return group;
    }
    int getIndex() {
        return index;
    }

    public boolean is_leaf() {
        return true;
    }

    public LEAF_OP(int index, Group group) {
        this.index = index;
        this.group = group;
    }

    public String getName() {
        return "LEAF_OP";
    }

    public LEAF_OP(LEAF_OP Op) {
        this.index = Op.index;
        this.group = Op.group;
    }

    public Op copy() {
        return new LEAF_OP(this);
    }

    public String toString() {
        return getName() + "<" + index + "," + group + ">";
    }
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        if (other == null || !(other instanceof LEAF_OP))
            return false;
        if (other.getClass() != LEAF_OP.class)
            return other.equals(this);
        LEAF_OP o = (LEAF_OP) other;
        return group.equals(o.group) && index == o.index;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return group.hashCode() ^ index;
    }

    /**
     * @see niagara.optimizer.colombia.Op#matches(Op)
     */
    public boolean matches(Op other) {
        if (other == null) return false;
        return true;
    }
}
