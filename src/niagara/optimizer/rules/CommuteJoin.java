/* $Id$ */
package niagara.optimizer.rules;

//import niagara.logical.EquiJoinPredicateList;
import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Predicate;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.joinOp;

/** Commute a remote subplan on the left ith a local subplan on the right */
public class CommuteJoin extends CustomRule {

    public CommuteJoin(String name) {
        super(
            name,
            2,
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(0)),
                new Expr(new LeafOp(1))),
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(1)),
                new Expr(new LeafOp(0))));
    }
    
    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        joinOp op = (joinOp) before.getOp();
        EquiJoinPredicateList eqPreds = op.getEquiJoinPredicates().reversed();
        Predicate pred = op.getNonEquiJoinPredicate().copy();

        return new Expr(
            new joinOp(pred, eqPreds),
            new Expr((before.getInput(1))),
            new Expr((before.getInput(0))));
    }
}
