/* $Id: SelectThruUnnest.java,v 1.1 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.logical.And;
import niagara.logical.Predicate;
import niagara.logical.True;
import niagara.logical.Unnest;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.selectOp;

/** Push parts of a predicate thru unnest */
public class SelectThruUnnest extends CustomRule {
    public SelectThruUnnest(String name) {
        super(
            name,
            1,
            new Expr(
                new selectOp(),
                new Expr(new Unnest(), new Expr(new LeafOp(0)))),
            new Expr(
                new selectOp(),
                new Expr(
                    new Unnest(),
                    new Expr(new selectOp(), new Expr(new LeafOp(0))))));
    }

    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        selectOp s = (selectOp) before.getOp();

        Expr unnestExpr = before.getInput(0);
        Expr leaf = unnestExpr.getInput(0);
        Unnest unnest = (Unnest) unnestExpr.getOp();

        Attrs leafAttrs =
            ((LeafOp) leaf.getOp()).getGroup().getLogProp().getAttrs();
        And splitPreds = s.getPredicate().split(leafAttrs);
        Predicate lowerPred = splitPreds.getLeft();
        Predicate upperPred = splitPreds.getRight();

        return new Expr(
            new selectOp(upperPred),
            new Expr(
                unnest,
                new Expr(new selectOp(lowerPred), new Expr(leaf))));
    }

    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        selectOp s = (selectOp) before.getOp();
        Expr leaf = before.getInput(0).getInput(0);
        Attrs leafAttrs =
            ((LeafOp) leaf.getOp()).getGroup().getLogProp().getAttrs();

        And splitPreds = s.getPredicate().split(leafAttrs);
        return !splitPreds.getLeft().equals(True.getTrue());
    }
}
