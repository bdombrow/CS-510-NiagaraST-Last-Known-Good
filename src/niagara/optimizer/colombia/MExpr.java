package niagara.optimizer.colombia;

import java.util.BitSet;

/**
   MULTI_EXPRESSIONS

   MExpr is a compact form of Expr which utilizes sharing.  Inputs are
   GROUPs instead of EXPRs, so the MExpr embodies several EXPRs.
   All searching is done with M_EXPRs, so each contains lots of state.
*/
public class MExpr {
    private BitSet ruleMask; //If 1, do not fire rule with that index

    private int counter; // how many winners point to this MEXPR
    private Op op; //Operator
    private Group[] inputs;
    private int grpID; //I reside in this group
    private Group group; // XXX vpapad: transition uses of group_id -> group
    public Group getGroup() {
        return group;
    }

    // link to the next mexpr in the same group 
    private MExpr nextMExpr;

    /*
    //This struct will be replaced with more efficient and flexible storage
    //Histor of which rules have been fired on this MExpr
            struct RuleHist {
            } RuleHist;
    */

    int getCounter() {
        return counter;
    }
    void incCounter() {
        counter++;
    }
    void decCounter() {
        if (counter != 0)
            counter--;
    }
    
    Op getOp() {
        return op;
    }
    
    public Group getInput(int i) {
        return inputs[i];
    }
    
    void setInput(int i, Group group) {
        inputs[i] = group;
    }
    int getGrpID() {
        return grpID;
    }
    int getArity() {
        return op.getArity();
    }

    void setNextMExpr(MExpr mExpr) {
        nextMExpr = mExpr;
    }
    MExpr getNextMExpr() {
        return nextMExpr;
    }

    //We just fired this rule, so update dont_fire bit vector
    public void fire_rule(int rule_no) {
        ruleMask.set(rule_no);
    }

    //Can I fire this rule?
    boolean can_fire(int rule_no) {
        return !ruleMask.get(rule_no);
    }

    // Transform an Expr into an MExpr.  
    // May involve creating new Groups.
    public MExpr(Expr expr, int grpid, SSP ssp) {
        op = expr.getOp().copy();
        grpID = (grpid == ssp.NEW_GRPID) ? ssp.getNewGrpID() : grpid;
        Expr input;
        counter = 0;
        ruleMask = new BitSet(ssp.getRulesetSize());

        // copy in the sub-expression
        int arity = getArity();
        if (arity != 0) {
            inputs = new Group[arity];
            for (int i = 0; i < arity; i++) {
                input = expr.getInput(i);

                if (input.getOp().is_leaf())
                    // deal with LEAF_OP, sharing the existed group
                    inputs[i] = ((LEAF_OP) input.getOp()).getGroup();
                else {
                    // create a new sub group
                    MExpr MExpr = ssp.CopyIn(input, ssp.NEW_GRPID);
                    inputs[i] = MExpr.getGroup();
                }
            }
        }
    }

    // copy constructor
    // for now, only use for copy into winner circle, 
    // so only consider physical mexpr, omitting some data members
    public MExpr(MExpr other) {
        op = other.getOp().copy();
        grpID = other.getGrpID();
        assert op.is_physical() || op.is_item();

        int arity = op.getArity();
        if (arity > 0) {
            inputs = new Group[arity];
            while (--arity >= 0)
                inputs[arity] = other.getInput(arity);
        }
    }

    // XXX vpapad: lousy hashcode implementation
    public int hashCode() {
        int hash = op.hashCode();
        for (int i = 0; inputs != null && i < inputs.length; i++)
            hash = hash ^ inputs[i].hashCode();
        return hash;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof MExpr))
            return false;
        MExpr mexpr = (MExpr) o;
        //XXX vpapad: Checking operators with equals, will they also be identical objects?
        if (!op.equals(mexpr.op) || inputs.length != mexpr.inputs.length)
            return false;
        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != mexpr.inputs[i])
                return false;
        }

        return true;
    }

    // String LightDump() {
    //         String os;
    //         String temp;
    //         os = (*Op).Dump();
    //         int Size = getArity();
    //         for(int i=0;i<Size;i++)
    //         {
    //                 temp.Format("%s%d", " , ", Inputs[i]);
    //                 os += temp;
    //         }
    //         return os;
    // }
    // String Dump()
    // {
    //         String os;
    //         String temp;
    //         os = (*Op).Dump();
    //         int Size = getArity();
    //         for(int i=0;i<Size;i++)
    //         {
    //                 temp.Format("%s%d", " , ", Inputs[i]);
    //                 os += temp;
    //         }
    //         temp += "[" + FindLocalCost().Dump() + "]; ";
    //         os += temp;
    //         return os;
    // }
    // Assisting Function -- will call PhysicalOp::FindLocalCost()
    // Cost FindLocalCost() {
    //         LogicalProperty LocalLogProp = Ssp.GetGroup(GetGrpID()).getLogProp();
    //         LogicalProperty **LogProps = new LogicalProperty* [getArity()];
    //         for(int i=0;i<getArity();i++)
    //         {
    //                 LogProps[i]=Ssp.GetGroup(GetInput(i)).getLogProp();
    //         }
    //         return ((PhysicalOp*)Op).FindLocalCost(LocalLogProp, LogProps);

    void clear_rule_mask() {
        ruleMask.xor(ruleMask);
    }

    void set_rule_mask(BitSet v) {
        ruleMask = v;
    }

    void add_rule_mask(BitSet v) {
        ruleMask.or(v);
    }

    /**
     * Sets the group.
     * @param group The group to set
     */
    public void setGroup(Group group) {
        this.group = group;
    }

    public String toString() {
        String opName = op.getName();
        if (inputs == null || inputs.length == 0)
            return opName;
        switch (inputs.length) {
            case 0 :
            case 1 :
                return "(" + opName + " " + inputs[0] + ")";
            case 2 :
                return "(" + inputs[0] + " " + opName + " " + inputs[1] + ")";
            default :
                String result = "(" + opName;
                for (int i = 0; i < inputs.length; i++)
                    result += " " + inputs[i];
                result += ")";
                return result;
        }
    }
}