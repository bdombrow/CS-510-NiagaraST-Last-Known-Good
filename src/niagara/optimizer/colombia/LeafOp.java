package niagara.optimizer.colombia;

/**
  *  Leaf operators, used in rules only.  Placeholder for a Group.
  */
public class LeafOp extends Op {

    private Group group;
    //Identifies the group bound to this leaf, after binding.
    // == null until binding
    private int index; //Used to distinguish this leaf in a rule

    public LeafOp(int index) {
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

    public LeafOp(int index, Group group) {
        this.index = index;
        this.group = group;
    }

    public String getName() {
        return "LeafOp";
    }

    public LeafOp(LeafOp Op) {
        this.index = Op.index;
        this.group = Op.group;
    }

    public Op copy() {
        return new LeafOp(this);
    }

    public String toString() {
        return getName() + "<" + index + "," + group + ">";
    }
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        if (other == null || !(other instanceof LeafOp))
            return false;
        if (other.getClass() != LeafOp.class)
            return other.equals(this);
        LeafOp o = (LeafOp) other;
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
