package niagara.optimizer.rules;

import niagara.logical.Pace;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.physical.PhysicalPace;

/** Union To PhysicalUnion implementation rule */
public class PaceToPhysicalPace extends CustomRule {
	// UnionToPhysicalUnion is a special case,
	// it must match union ops with any number of inputs
	public PaceToPhysicalPace(String name) {
		super(name, 0, new Expr(new Pace()) {
			public Expr getInput(int i) {
				return new Expr(new LeafOp(i));
			}
		}, new Expr(new PhysicalPace()));
	}

	public Expr nextSubstitute(Expr before, MExpr mexpr,
			PhysicalProperty ReqdProp) {
		Expr[] inputs = new Expr[before.getArity()];
		for (int i = 0; i < inputs.length; i++)
			inputs[i] = before.getInput(i);
		PhysicalPace op = new PhysicalPace();
		op.initFrom((LogicalOp) before.getOp());
		return new Expr(op, inputs);
	}
}
