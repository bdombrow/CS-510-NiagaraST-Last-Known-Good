/* $Id: PushJoinPredicates.java,v 1.4 2003/09/13 03:44:02 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.logical.And;
import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Predicate;
import niagara.logical.True;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.joinOp;
import niagara.logical.Select;

/** Push as many of the non-equijoin predicates of a join operator
 * to its inputs */
public class PushJoinPredicates extends CustomRule {
    public PushJoinPredicates(String name) {
        super(
            name,
            2,
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(0)),
                new Expr(new LeafOp(1))),
                // This is not necessarily true, we may be able
                // to push predicates down only to one of the leaves
            new Expr(
                new joinOp(),
                new Expr(new Select(),
                    new Expr(new LeafOp(0)),
                new Expr(new Select(),
                    new Expr(new LeafOp(1))))));
    }

    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        joinOp j = (joinOp) before.getOp();
        Expr leftLeaf = before.getInput(0);
        Expr rightLeaf = before.getInput(1);
        Attrs leftAttrs =
            ((LeafOp) leftLeaf.getOp()).getGroup().getLogProp().getAttrs();
        Attrs rightAttrs =
            ((LeafOp) rightLeaf.getOp()).getGroup().getLogProp().getAttrs();

        Predicate pred = j.getNonEquiJoinPredicate();
        
        And splitLeft = pred.split(leftAttrs);
        Predicate pushLeft = splitLeft.getLeft();
        Predicate remLeft = splitLeft.getRight();
        
        And splitRight = remLeft.split(rightAttrs);
        Predicate pushRight = splitRight.getLeft();
        Predicate remJoin = splitRight.getRight();

        Expr left = leftLeaf;
        if (!pushLeft.equals(True.getTrue()))        
            left = new Expr(new Select(pushLeft), left);
        Expr right = rightLeaf;
        if (!pushRight.equals(True.getTrue()))        
            right = new Expr(new Select(pushRight), right);

        EquiJoinPredicateList eq = j.getEquiJoinPredicates();
        joinOp newJ = new joinOp(remJoin, eq, j.getProjectedAttrs(),
				 j.getExtensionJoin());
        return new Expr(newJ, left, right);
    }
    
    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        joinOp j = (joinOp) before.getOp();
        Expr leftLeaf = before.getInput(0);
        Expr rightLeaf = before.getInput(1);
        Attrs leftAttrs =
            ((LeafOp) leftLeaf.getOp()).getGroup().getLogProp().getAttrs();
        Attrs rightAttrs =
            ((LeafOp) rightLeaf.getOp()).getGroup().getLogProp().getAttrs();

        return j.hasPushablePredicates(leftAttrs, rightAttrs);               
    }
}
