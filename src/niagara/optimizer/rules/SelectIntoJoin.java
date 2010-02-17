package niagara.optimizer.rules;

import niagara.logical.Join;
import niagara.logical.Select;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;

public class SelectIntoJoin extends CustomRule {
	public SelectIntoJoin(String name) {
		super(name, 2, new Expr(new Select(), new Expr(new Join(), new Expr(
				new LeafOp(0)), new Expr(new LeafOp(1)))), new Expr(new Join(),
				new Expr(new LeafOp(0)), new Expr(new LeafOp(1))));
	}

	public Expr nextSubstitute(Expr before, MExpr mexpr,
			PhysicalProperty ReqdProp) {
		Select s = (Select) before.getOp();
		Predicate sPred = s.getPredicate();

		Expr joinExpr = before.getInput(0);
		Expr leftLeaf = joinExpr.getInput(0);
		Expr rightLeaf = joinExpr.getInput(1);
		Join oldJ = (Join) joinExpr.getOp();
		Attrs leftAttrs = ((LeafOp) leftLeaf.getOp()).getGroup().getLogProp()
				.getAttrs();
		Attrs rightAttrs = ((LeafOp) rightLeaf.getOp()).getGroup().getLogProp()
				.getAttrs();

		Join newJ = oldJ.withExtraCondition(sPred, leftAttrs, rightAttrs);

		return new Expr(newJ, new Expr(leftLeaf), new Expr(rightLeaf));
	}
}
