package niagara.optimizer.rules;

import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;

public class ConsolidatingAssociateJoinRtoL extends AssociateJoinRtoL {
	public ConsolidatingAssociateJoinRtoL(String name) {
		super(name);
	}

	public boolean condition(Expr before, MExpr mexpr, PhysicalProperty ReqdProp) {

		if (!mexpr.getGroup().getLogProp().isMixed())
			return false;

		boolean aLocal = ((LeafOp) (before.getInput(0).getOp())).getGroup()
				.getLogProp().isLocal();

		boolean bLocal = ((LeafOp) (before.getInput(1).getInput(0).getOp()))
				.getGroup().getLogProp().isLocal();

		boolean cLocal = ((LeafOp) (before.getInput(1).getInput(1).getOp()))
				.getGroup().getLogProp().isLocal();

		// L1 x (L2 x R) -> (L1 x L2) x R
		if (aLocal && bLocal && !cLocal)
			return true;
		// L x (R1 x R2) -> (L1 x R1) x R2
		if (aLocal && !bLocal && !cLocal)
			return true;
		// R1 x (L x R2) -> (R1 x L1) x R2
		if (!aLocal && bLocal && !cLocal)
			return true;
		// R1 x (R2 x R3) -> (R1 x R2) x R3
		if (!aLocal && !bLocal && !cLocal)
			return true;

		return false;
	}
}
