package niagara.optimizer.rules;

import niagara.optimizer.AnyLogicalOp;
import niagara.optimizer.ConsolidateMe;
import niagara.optimizer.colombia.*;

/** ConsolidateMeRule maps a non-consolidated operator to the
 * ConsolidateMe pseudo-physical operator, forcing Colombia to
 * consolidate its inputs */
public class ConsolidateMeRule extends CustomRule {
    public ConsolidateMeRule(String name) {
        super(
            name,
            0,
            new Expr(new AnyLogicalOp()) {
                public Expr getInput(int i) {
                    return new Expr(new LeafOp(i));
                }
            },
            new Expr(new ConsolidateMe(0)));
        }

    /**
     * @see niagara.optimizer.colombia.Rule#next_substitute(Expr, MExpr, PhysicalProperty)
     */
    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        int arity = before.getArity();
        Expr[] inputs = new Expr[arity];
        for (int i = 0; i < arity; i++)
            inputs[i] = before.getInput(i);
        return new Expr(new ConsolidateMe(arity), inputs);
    }

    /**
     * @see niagara.optimizer.colombia.Rule#condition(Expr, MExpr, PhysicalProperty)
     */
    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        return mexpr.getGroup().getLogProp().isMixed();
    }
}
