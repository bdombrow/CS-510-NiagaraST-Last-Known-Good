/* $Id: SelectIntoJoin.java,v 1.2 2003/03/07 21:00:32 tufte Exp $ */
package niagara.optimizer.rules;

import niagara.logical.Predicate;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.joinOp;
import niagara.logical.Select;

public class SelectIntoJoin extends CustomRule {
    public SelectIntoJoin(String name) {
        super(
            name,
            2,
            new Expr(
                new Select(),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(1)))),
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(0)),
                new Expr(new LeafOp(1))));
    }
    
    
    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        Select s = (Select) before.getOp();
        Predicate sPred = s.getPredicate();
        
        Expr joinExpr = before.getInput(0);
        Expr leftLeaf = joinExpr.getInput(0);
        Expr rightLeaf = joinExpr.getInput(1);
        joinOp oldJ = (joinOp) joinExpr.getOp();
        Attrs leftAttrs = ((LeafOp) leftLeaf.getOp()).getGroup().getLogProp().getAttrs();
        Attrs rightAttrs = ((LeafOp) rightLeaf.getOp()).getGroup().getLogProp().getAttrs();
        
        joinOp newJ = oldJ.withExtraCondition(sPred, leftAttrs, rightAttrs);
        
        return new Expr(
            newJ,
            new Expr(leftLeaf),
            new Expr(rightLeaf));
    }
}
