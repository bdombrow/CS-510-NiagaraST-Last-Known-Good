/* $Id: SelectIntoJoin.java,v 1.4 2003/12/24 01:51:56 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.*;
import niagara.logical.Select;
import niagara.logical.Join;
import niagara.logical.predicates.Predicate;

public class SelectIntoJoin extends CustomRule {
    public SelectIntoJoin(String name) {
        super(
            name,
            2,
            new Expr(
                new Select(),
                new Expr(
                    new Join(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(1)))),
            new Expr(
                new Join(),
                new Expr(new LeafOp(0)),
                new Expr(new LeafOp(1))));
    }
    
    
    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        Select s = (Select) before.getOp();
        Predicate sPred = s.getPredicate();
        
        Expr joinExpr = before.getInput(0);
        Expr leftLeaf = joinExpr.getInput(0);
        Expr rightLeaf = joinExpr.getInput(1);
        Join oldJ = (Join) joinExpr.getOp();
        Attrs leftAttrs = ((LeafOp) leftLeaf.getOp()).getGroup().getLogProp().getAttrs();
        Attrs rightAttrs = ((LeafOp) rightLeaf.getOp()).getGroup().getLogProp().getAttrs();
        
        Join newJ = oldJ.withExtraCondition(sPred, leftAttrs, rightAttrs);
        
        return new Expr(
            newJ,
            new Expr(leftLeaf),
            new Expr(rightLeaf));
    }
}
