/* $Id: PushProjection.java,v 1.4 2005/08/26 16:43:51 vpapad Exp $ */
package niagara.optimizer.rules;

import niagara.logical.Project;
import niagara.logical.LogicalOperator;
import niagara.optimizer.AnyLogicalOp;
import niagara.optimizer.NoOp;
import niagara.optimizer.colombia.*;

/** Push Project through a logical operator */
public class PushProjection extends CustomRule {
    public PushProjection(String name) {
        super(name, 0, // Hack to match operators with any arity
        new Expr(new Project(), new Expr(new AnyLogicalOp()) {
            public Expr getInput(int i) {
                return new Expr(new LeafOp(i));
            }
        }),
        // XXX vpapad: The output pattern is meaningless
        new Expr(new AnyLogicalOp()));
    }

    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // xop is the logical operator beneath project
        Expr xexpr = before.getInput(0);
        LogicalOperator xop = (LogicalOperator) xexpr.getOp();
        int arity = xexpr.getArity();

        // Get the schemas of x's inputs
        LeafOp[] leaves = new LeafOp[arity];
        Attrs[] inputAttrs = new Attrs[arity];
        for (int i = 0; i < arity; i++) {
            leaves[i] = (LeafOp) xexpr.getInput(i).getOp();
            inputAttrs[i] = leaves[i].getGroup().getLogProp().getAttrs();
        }

        // Get attributes we want to project
        Project p = ((Project) before.getOp());
        Attrs projectAttrs = p.getAttrs();

        // All input attributes to xop
        Attrs allAttrs = new Attrs();
        for (int i = 0; i < arity; i++)
            allAttrs.merge(inputAttrs[i]);

        Op orgOp = xop.copy();
        
        // If there are any attributes in (allAttrs - projectAttrs)
        // then the implementation of xop can project them away
        if (arity == 0 || allAttrs.minus(projectAttrs).size() != 0)
            xop.projectedOutputAttributes(projectAttrs);

        // Have we changed anything?
        boolean changed = (!xop.equals(orgOp));
        
        // Find all attributes required by xop and add them
        // to projectAttrs - those are the attributes we
        // want to keep coming into xop
        projectAttrs.merge(xop.requiredInputAttributes(allAttrs));

        // For every input, we add a project if 
        // (inputs[i] - projectAttrs) is non-empty
        Expr inputs[] = new Expr[arity];

        for (int i = 0; i < arity; i++) {
            Attrs a = inputAttrs[i].project(projectAttrs);
            if (!a.equals(inputAttrs[i])) {
                changed = true;
                inputs[i] = new Expr(new Project(a), new Expr(leaves[i]));
            }
            else
                inputs[i] = new Expr(leaves[i]);
        }

        Expr result = new Expr(xop, inputs);
        
        if (changed) // If we created a new expression, return it
            return result;
        else // otherwise, replace the project with a noop
            return new Expr(new NoOp(), result);
    }
}
