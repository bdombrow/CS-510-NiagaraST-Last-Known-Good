/* $Id$ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.*;

public class ConsolidatingAssociateJoinRtoL extends AssociateJoinRtoL {
    public ConsolidatingAssociateJoinRtoL() {
        super("ConsolidatingAssociateJoinRtoL");
    }
    
    public Rule copy() {
        return new ConsolidatingAssociateJoinRtoL();
    }
    
    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {

        if (!mexpr.getGroup().getLogProp().isMixed()) return false;
        
        boolean aLocal =
            ((LEAF_OP) (before.getInput(0).getOp()))
                .getGroup()
                .getLogProp()
                .isLocal();

        boolean bLocal =
            ((LEAF_OP) (before.getInput(1).getInput(0).getOp()))
                .getGroup()
                .getLogProp()
                .isLocal();
            
        boolean cLocal =
            ((LEAF_OP) (before.getInput(1).getInput(1).getOp()))
                .getGroup()
                .getLogProp()
                .isLocal();

        // L1 x (L2 x R) -> (L1 x L2) x R
        if (aLocal && bLocal && !cLocal) return true;
        // L x (R1 x R2) -> (L1 x R1) x R2
        if (aLocal && !bLocal && !cLocal) return true;
        // R1 x (L x R2) -> (R1 x L1) x R2
        if (!aLocal && bLocal && !cLocal) return true;
        // R1 x (R2 x R3) -> (R1 x R2) x R3
        if (!aLocal && !bLocal && !cLocal) return true;

        return false;        
    }
}
