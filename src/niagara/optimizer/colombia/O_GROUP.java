package niagara.optimizer.colombia;

import java.util.ArrayList;

/***
 * ============================================================ O_GROUP - Task
 * to Optimize a Group
 * ============================================================ This task finds
 * the cheapest multiplan in this group, for a given context, and stores it
 * (with the context) in the group's winner's circle. If there is no cheapest
 * plan (e.g. the upper bound cannot be met), the context is stored in the
 * winner's circle with a null plan.
 * 
 * This task generates all relevant expressions in the group, costs them and
 * chooses the cheapest one.
 * 
 * The determination of what is "cheapest" may include such issues as
 * robustness, e.g. this task may choose the multiplan with smallest
 * cost+variance. Here variance is some measure of how much the cost varies as
 * the statistics vary.
 * 
 * There are at least two ways to implement this task; we will discover which is
 * best.
 * 
 * First, Goetz' original way, is to process each multiexpression separately, in
 * the order they appear in the list/collection of multiexpressions. To process
 * an expression means to determine all relevant rules, then fire them in order
 * of promise().
 * 
 * Second, hinted at by Goetz, applies only when there are multiple expressions
 * in the group, e.g. after exploring. Here we can consider all rules on all
 * expressions, and fire them in order of their promise. Promise here may
 * include a lower bound estimate for the expressions.
 */
@SuppressWarnings("unchecked")
public class O_GROUP extends Task {

	private Group group; // Which group to optimize
	//private boolean Last; // if this task is the last task for this group
	private Cost epsBound;

	// if global eps pruning is on, this is the eps bound for eps pruning
	// else it is zero

	public O_GROUP(SSP ssp, Group group, Context context, boolean last,
			Cost bound) {
		super(ssp, context);
		this.group = group;
		//this.Last = last;
		this.epsBound = bound;

		// if INFBOUND flag is on, set the bound to be INF
		if (ssp.INFBOUND) {
			Cost INFCost = new Cost(-1);
			context.setUpperBound(INFCost);
		}
	}

	public O_GROUP(SSP ssp, Group group, Context context) {
		this(ssp, group, context, true, null);
	}

	/*
	 * perform { see search_circle in declaration of class Group, for notation
	 * 
	 * Call search_circle If cases (1) or (2), terminate this task. //circle is
	 * prepared Cases (3) and (4) remain. More search.
	 * 
	 * IF (Group is not optimized) assert (this is case 3) if (property is ANY)
	 * add a winner for this context to the circle, with null plan and infinite
	 * cost. (i.e., initialize the winner's circle for this property.) Push
	 * O_EXPR on first logical expression else Push O_GROUP on this group with
	 * current context Push O_GROUP on this group, with new context: ANY
	 * property and cost = current context cost - appropriate enforcer cost,
	 * last task else (Group is optimized) if (property is ANY) assert (this is
	 * case 4) push O_INPUTS on all physical mexprs else (property is not ANY)
	 * Push O_INPUTS on all physical mexprs with current context, last one is
	 * last task If case (3) [i.e. appropriate enforcer is not in group], Push
	 * ApplyRule on enforcer rule, not the last task add a winner for this
	 * context to the circle, with null plan. (i.e., initialize the winner's
	 * circle for this property.)
	 */
	public void perform() {
		// PTRACE ("O_GROUP %d performing", GrpID);

		// PTRACE ("Last flag is %d", Last);

		Group Group = group;
		MExpr FirstLogMExpr = Group.getFirstLogMExpr();

		Context LocalCont = context;
		PhysicalProperty LocalReqdProp = LocalCont.getPhysProp();

		Group.SearchResults sr = Group.search_circle(LocalCont);

		// If case (2) or (1), terminate this task
		if (sr.isImpossible() || sr.haveWinner()) {
			// PTRACE("%s",
			// "Winner's circle is prepared so terminate this task");
			return;
		}

		// PTRACE("Group is %s optimized", Group.is_optimized() ? "" : "not");
		if (!Group.isOptimized()) {
			assert (sr.isStartingSearch()); // assert (this is case 3)
			// if (property is ANY)
			if (LocalReqdProp.getOrder().isAny()) {
				// PTRACE("%s","add winner with null plan, push O_EXPR on 1st logical expression");
				Group.newWinner(LocalReqdProp, null, new Cost(-1), false);
				if (ssp.GlobepsPruning) {
					Cost eps_bound = new Cost(epsBound);
					ssp.addTask(new O_EXPR(ssp, FirstLogMExpr, false, context,
							true, eps_bound));
				} else
					ssp.addTask(new O_EXPR(ssp, FirstLogMExpr, false, context,
							true));
			} else {
				// // XXX vpapad: if it's not any, must be sorted?
				// XXX vpapad commenting out, seems to depend on
				// knowing about specific physical properties,
				// and specific ways to produce them (MSORT for sorted)
				assert false : "XXX vpapad can't handle properties other than ANY";
				// // PTRACE(
				// // "%s",
				// //"Push O_GROUP with current context, another with ANY context");
				// assert LocalReqdProp.getOrder().getKind() ==
				// Order.KIND.SORTED;
				// //temporary
				// if (ssp.GlobepsPruning) {
				// Cost eps_bound = new Cost(epsBound);
				// ssp.addTask(
				// new O_GROUP(ssp, GrpID, ContextID, ssp.TaskNo, true,
				// eps_bound));
				// } else
				// ssp.addTask(new O_GROUP(ssp, GrpID, ContextID, ssp.TaskNo));
				//
				// if (LocalReqdProp.getOrder().getKind() == Order.KIND.SORTED)
				// {
				// Cost NewCost = new Cost((LocalCont.getUpperBd()));
				// LogicalProperty LogProp = Group.getLogProp();
				// MSORT Qsort;
				// Cost SortCost = Qsort.FindLocalCost(LogProp, LogProp);
				// NewCost -= SortCost;
				//
				// Context NewContext =
				// new Context(new PhysicalProperty(new Order(any)), NewCost,
				// false);
				// ssp.vc.add(NewContext);
				// if (GlobepsPruning) {
				// Cost eps_bound = new Cost(EpsBound);
				// PTasks.push(
				// new O_GROUP(
				// GrpID,
				// ssp.vc.size() - 1,
				// TaskNo,
				// true,
				// eps_bound));
				// } else
				// PTasks.push(
				// new O_GROUP(
				// GrpID,
				// ssp.vc.size() - 1,
				// TaskNo,
				// true));
				// } else {
				// assert false; //since for now sort only
				// }
			}
		} else { // Group is optimized
			// if (property is ANY)
			// assert (this is case 4)
			// push O_INPUTS on all physical mexprs
			ArrayList PhysMExprs = new ArrayList();
			int count = 0;
			if (LocalReqdProp.getOrder().getKind() == Order.Kind.ANY) {
				// PTRACE("%s", "push O_INPUTS on all physical mexprs");
				assert sr.isContinuingSearch();
				for (MExpr PhysMExpr = Group.getFirstPhysMExpr(); PhysMExpr != null; PhysMExpr = PhysMExpr
						.getNextMExpr()) {
					PhysMExprs.add(PhysMExpr);
					count++;
				}
				// push the last PhysMExpr
				if (--count >= 0) {
					// PTRACE0("pushing O_INPUTS " + PhysMExprs[count].Dump());
					if (ssp.GlobepsPruning) {
						Cost eps_bound = new Cost(epsBound);
						ssp.addTask(new O_INPUTS((MExpr) PhysMExprs.get(count),
								context, true, eps_bound));
					} else
						ssp.addTask(new O_INPUTS((MExpr) PhysMExprs.get(count),
								context, true));
				}
				// push other PhysMExpr
				while (--count >= 0) {
					// PTRACE0("pushing O_INPUTS " + PhysMExprs[count].Dump());
					if (ssp.GlobepsPruning) {
						Cost eps_bound = new Cost(epsBound);
						ssp.addTask(new O_INPUTS((MExpr) PhysMExprs.get(count),
								context, false, eps_bound));
					} else
						ssp.addTask(new O_INPUTS((MExpr) PhysMExprs.get(count),
								context, false));
				}
			} else { // (property is not ANY)
				assert false : "XXX vpapad can't handle properties other than ANY";
				// // Push O_INPUTS on all physical mexprs with current context,
				// // last one is last task
				// //PTRACE("%s", "Push O_INPUTS on all physical mexprs");
				// for (MExpr PhysMExpr = Group.GetFirstPhysMExpr();
				// PhysMExpr;
				// PhysMExpr = PhysMExpr.GetNextMExpr()) {
				// PhysMExprs.push_back(PhysMExpr);
				// count++;
				// }
				// //push the last PhysMExpr
				// if (--count >= 0) {
				// PTRACE0("pushing O_INPUTS " + PhysMExprs[count].Dump());
				// if (GlobepsPruning) {
				// Cost eps_bound = new Cost(EpsBound);
				// PTasks.push(
				// new O_INPUTS(
				// PhysMExprs.get(count),
				// ContextID,
				// TaskNo,
				// true,
				// eps_bound));
				// } else
				// PTasks.push(
				// new O_INPUTS(
				// PhysMExprs[count],
				// ContextID,
				// TaskNo,
				// true));
				// }
				// //push other PhysMExpr
				// while (--count >= 0) {
				// PTRACE0("pushing O_INPUTS " + PhysMExprs[count].Dump());
				// if (GlobepsPruning) {
				// Cost eps_bound = new Cost(EpsBound);
				// PTasks.push(
				// new O_INPUTS(
				// PhysMExprs[count],
				// ContextID,
				// TaskNo,
				// false,
				// eps_bound));
				// } else
				// PTasks.push(
				// new O_INPUTS(
				// PhysMExprs[count],
				// ContextID,
				// TaskNo,
				// false));
				// }
				//
				// // If case (3) [i.e. appropriate enforcer is not in group],
				// // Push ApplyRule on enforcer rule, not the last task
				// if (!SCReturn) {
				// PTRACE("%s", "Push ApplyRule on enforcer rule");
				// if (LocalReqdProp.GetOrder().GetKind() == sorted) {
				// Rule Rule = (RuleSet)[R_SORT_RULE];
				// if (GlobepsPruning) {
				// Cost eps_bound = new Cost(EpsBound);
				// PTasks.push(
				// new ApplyRule(
				// Rule,
				// FirstLogMExpr,
				// false,
				// ContextID,
				// TaskNo));
				// } else
				// PTasks.push(
				// new ApplyRule(
				// Rule,
				// FirstLogMExpr,
				// false,
				// ContextID,
				// TaskNo,
				// false));
				// } else {
				// assert(false);
				// }
				// }
				// // add a winner to the circle, with null plan.
				// //(i.e., initialize the winner's circle for this property.)
				// //PTRACE("%s", "Init winner's circle for this property");
				// if (moreSearch && !SCReturn)
				// Group.NewWinner(LocalReqdProp, null, new Cost(-1), false);
			}
		}
	} // perform

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "Optimizing " + group;
	}

} // O_GROUP
