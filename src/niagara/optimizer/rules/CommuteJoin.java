/* $Id$ */
package niagara.optimizer.rules;

import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Predicate;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.op_tree.joinOp;

/** Commute a remote subplan on the left ith a local subplan on the right */
public class CommuteJoin extends Rule {
    public CommuteJoin() {
        this("CommuteJoin");
    }

    protected CommuteJoin(String name) {
        super(
            name,
            2,
            new Expr(
                new joinOp(),
                new Expr(new LEAF_OP(0)),
                new Expr(new LEAF_OP(1))),
            new Expr(
                new joinOp(),
                new Expr(new LEAF_OP(1)),
                new Expr(new LEAF_OP(0))));
    }
    
    public Rule copy() {
        return new CommuteJoin();
    }
    
    public void initialize() {
        maskRule("CommuteJoin");
    }
    
// XXX vpapad hack 
    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
            return null;
        }

/*

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
*/    
}
