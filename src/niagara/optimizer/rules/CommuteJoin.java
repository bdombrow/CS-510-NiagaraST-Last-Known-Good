package niagara.optimizer.rules;

import niagara.logical.EquiJoinPredicateList;
import niagara.logical.Join;
import niagara.logical.predicates.Predicate;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;

/** Commute a remote subplan on the left ith a local subplan on the right */
public class CommuteJoin extends CustomRule {

	public CommuteJoin(String name) {
		super(name, 2, new Expr(new Join(), new Expr(new LeafOp(0)), new Expr(
				new LeafOp(1))), new Expr(new Join(), new Expr(new LeafOp(1)),
				new Expr(new LeafOp(0))));
	}

	public Expr nextSubstitute(Expr before, MExpr mexpr,
			PhysicalProperty ReqdProp) {
		Join op = (Join) before.getOp();
		EquiJoinPredicateList eqPreds = op.getEquiJoinPredicates().reversed();
		Predicate pred = op.getNonEquiJoinPredicate();

		int newExtJoin = Join.NONE;
		switch (op.getExtensionJoin()) {
		case Join.LEFT:
			newExtJoin = Join.RIGHT;
			break;
		case Join.RIGHT:
			newExtJoin = Join.LEFT;
			break;
		case Join.NONE:
			newExtJoin = Join.NONE;
			break;
		case Join.BOTH:
			newExtJoin = Join.BOTH;
			break;
		}

		return new Expr(new Join(pred, eqPreds, newExtJoin), new Expr((before
				.getInput(1))), new Expr((before.getInput(0))));
	}
}
