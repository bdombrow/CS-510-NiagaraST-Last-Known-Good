/* $Id: UnionToPhysicalUnion.java,v 1.2 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.*;
import niagara.query_engine.PhysicalUnionOperator;
import niagara.xmlql_parser.op_tree.UnionOp;

/** Union To PhysicalUnion implementation rule */
public class UnionToPhysicalUnion extends CustomRule {
    // UnionToPhysicalUnion is a special case,
    // it must match union ops with any number of inputs
    public UnionToPhysicalUnion(String name) {
        super(
            name,
            0,
            new Expr(new UnionOp()) {
                public Expr getInput(int i) {
                    return new Expr(new LeafOp(i));
                }
            },
            new Expr(new PhysicalUnionOperator()));
    }
    
    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
            Expr[] inputs = new Expr[before.getArity()];
            for (int i = 0; i < inputs.length; i++)
                inputs[i] = before.getInput(i);
                PhysicalUnionOperator op = new PhysicalUnionOperator();
                op.initFrom((LogicalOp) before.getOp());
        return new Expr(op, inputs);
    }
}
