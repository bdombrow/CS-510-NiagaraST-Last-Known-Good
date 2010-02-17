package niagara.optimizer.colombia;

/**
 * Winner
 * 
 * The key idea of dynamic programming/memoization is to save the results of
 * searches for future use. A Winner is such a result. In general a Winner
 * contains the MExpr which won a search plus the context (Context) used in the
 * search. Done = False, in a winner, means this winner is under construction;
 * its search is not complete.
 * 
 * Each group has a set of winners derived from previous searches of that group.
 * This set of winners is called a memo in the classic literature; here we call
 * it a winner's circle (cf. the Group class).
 * 
 * A winner can represent these cases, if Done is true: (1) If MPlan is not a
 * null pointer: MPlan is the cheapest possible plan in this group with
 * PhysProp. MPlan has cost *Cost. This derives from a successful search. (2) If
 * MPlan is a null pointer, and Cost is not null: All possible plans in this
 * group with PhysProp cost more than *Cost. This derives from a search which
 * fails because of cost. (3) If MPlan is a null pointer, and Cost is null:
 * There can be no plan in this group with PhysProp (Should never happen if we
 * have enforcers)
 * 
 * While the physical mexpressions of a group are being costed (i.e.
 * Done=false), the cheapest yet found is stored in a winner.
 */
public class Winner {

	private MExpr mPlan;
	// physProp and cost typically represent the context of the search
	// which generated this winner.
	private PhysicalProperty physProp;
	private Cost cost;

	private boolean done; // Is this a real winner?

	public Winner(MExpr mexpr, PhysicalProperty physProp, Cost cost,
			boolean done) {
		this.cost = cost;
		if (mexpr == null)
			mPlan = null;
		else
			mPlan = new MExpr(mexpr);
		this.physProp = physProp;
		this.done = done;
	}

	public MExpr getMPlan() {
		return mPlan;
	}

	public PhysicalProperty getPhysProp() {
		return physProp;
	}

	public Cost getCost() {
		return cost;
	}

	public boolean getDone() {
		return done;
	}

	public void setDone() {
		done = true;
	}
}
