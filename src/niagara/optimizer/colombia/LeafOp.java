/* $Id: LeafOp.java,v 1.3 2003/07/03 19:43:39 tufte Exp $ 
   Colombia -- Java version of the Columbia Database Optimization Framework

   Copyright (c)    Dept. of Computer Science , Portland State
   University and Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS, THE DEPT. OF COMPUTER SCIENCE DEPT. OF PORTLAND STATE
   UNIVERSITY AND DEPT. OF COMPUTER SCIENCE & ENGINEERING AT OHSU ALLOW
   USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, AND THEY DISCLAIM ANY
   LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE
   USE OF THIS SOFTWARE.

   This software was developed with support of NSF grants IRI-9118360,
   IRI-9119446, IRI-9509955, IRI-9610013, IRI-9619977, IIS 0086002,
   and DARPA (ARPA order #8230, CECOM contract DAAB07-91-C-Q518).
*/
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

    public Op opCopy() {
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
