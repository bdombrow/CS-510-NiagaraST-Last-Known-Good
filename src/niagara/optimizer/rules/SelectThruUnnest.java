/* $Id: SelectThruUnnest.java,v 1.4 2003/12/24 01:51:56 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.logical.Unnest;
import niagara.optimizer.colombia.*;
import niagara.logical.Select;
import niagara.logical.predicates.And;
import niagara.logical.predicates.Predicate;
import niagara.logical.predicates.True;

/** Push parts of a predicate thru unnest */
public class SelectThruUnnest extends CustomRule {
    public SelectThruUnnest(String name) {
        super(
            name,
            1,
            new Expr(
                new Select(),
                new Expr(new Unnest(), new Expr(new LeafOp(0)))),
            new Expr(
                new Select(),
                new Expr(
                    new Unnest(),
                    new Expr(new Select(), new Expr(new LeafOp(0))))));
    }

    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        Select s = (Select) before.getOp();

        Expr unnestExpr = before.getInput(0);
        Expr leaf = unnestExpr.getInput(0);
        Unnest unnest = (Unnest) unnestExpr.getOp();

        Attrs leafAttrs =
            ((LeafOp) leaf.getOp()).getGroup().getLogProp().getAttrs();
        And splitPreds = s.getPredicate().split(leafAttrs);
        Predicate lowerPred = splitPreds.getLeft();
        Predicate upperPred = splitPreds.getRight();

        return new Expr(
            new Select(upperPred),
            new Expr(
                unnest,
                new Expr(new Select(lowerPred), new Expr(leaf))));
    }

    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        Select s = (Select) before.getOp();
        Expr leaf = before.getInput(0).getInput(0);
        Attrs leafAttrs =
            ((LeafOp) leaf.getOp()).getGroup().getLogProp().getAttrs();

        And splitPreds = s.getPredicate().split(leafAttrs);
        return !splitPreds.getLeft().equals(True.getTrue());
    }
}
