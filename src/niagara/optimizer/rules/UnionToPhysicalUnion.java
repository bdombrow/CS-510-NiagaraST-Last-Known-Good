/* $Id: UnionToPhysicalUnion.java,v 1.1 2002/10/31 04:35:24 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.optimizer.colombia.*;
import niagara.query_engine.PhysicalUnionOperator;
import niagara.xmlql_parser.op_tree.UnionOp;

/** Union To PhysicalUnion implementation rule */
public class UnionToPhysicalUnion extends Rule {
    // UnionToPhysicalUnion is a special case,
    // it must match union ops with any number of inputs
    public UnionToPhysicalUnion() {
        super(
            "UnionToPhysicalUnion",
            0,
            new Expr(new UnionOp()) {
                public Expr getInput(int i) {
                    return new Expr(new LEAF_OP(i));
                }
            },
            new Expr(new PhysicalUnionOperator()));
    }
    
    public Rule copy() {
        return new UnionToPhysicalUnion();
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
