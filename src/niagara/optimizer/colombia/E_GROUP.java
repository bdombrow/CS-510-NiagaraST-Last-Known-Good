package niagara.optimizer.colombia;

/*
============================================================
E_GROUP - Task to Explore the Group
============================================================
Some rules require that their inputs contain particular (target) operators.  For
example, the associativity rule requires that one input contain a join.  The
E_GROUP task explores a group by creating all target operators that could
belong to the group, e.g., fire whatever rules are necessary to create all
joins that could belong to the group.

	 The simplest implementation of this task is to generate all logical 
	 multiexpressions in the group.  
	 
	   More sophisticated implementations would fire only those rules which might 
	   generate the target operator.  But it is hard to tell what those rules
	   are (note that it may require a sequence of rules to get to the target).
	   Furthermore, on a second E_GROUP task for a second target it may be
	   difficult to use the results of the first visit intelligently.
	   
		 Because we are using the simple implementation, we do not need an E_EXPR task.
		 Instead we will use the O_GROUP task but ensure that it fires only transformation rules.
		 
		   If we are lucky, groups will never need to be explored: physical rules are fired first,
		   and the firing of a physical rule will cause all inputs to be optimized, therefore explored.
		   This may not work if we are using pruning: we might skip physical rule firings because of
		   pruning, then need to explore.  For now we will put a flag in E_GROUP to catch when it does not work.
*/

public class E_GROUP extends Task {

    private Group group; //Group to be explored
    boolean Last; // is it the last task in this group
    Cost EpsBound;
    // if global eps pruning is on, this is the eps bound for eps pruning
    // else it is zero

    //    Task to explore a group
    E_GROUP(SSP ssp,
        Group group,
        int ContextID,
        boolean last,
        Cost bound) {
        super(ssp, ContextID);
        this.group = group;
        this.Last = last;
        this.EpsBound = bound;
    }

    E_GROUP(SSP ssp, Group group, int ContextID) {
        this(ssp, group, ContextID, false, null);
    }

    public void perform() {

        //        PTRACE("E_GROUP %d performing", GrpID);
        //        PTRACE2(
        //            "Context ID: %d , %s",
        //            ContextID,
        //            (const char *) Context : : vc[ContextID].Dump());

        Group Group = group;

        if (Group.isOptimized() || Group.isExplored()) {
            //See discussion in E_GROUP class declaration
            return;
        }

        if (Group.is_exploring())
            assert false;
        else {
            // the group will be explored, let other tasks don't do it again
            Group.set_exploring(true);

            // mark the group not explored since we will begin exploration
            Group.setExplored(false);

            MExpr LogMExpr = Group.getFirstLogMExpr();

            // only need to E_EXPR the first log expr, 
            // because it will generate all logical exprs by applying appropriate rules
            // it won't generate dups because rule bit vector 
//            PTRACE0("pushing O_EXPR exploring " + LogMExpr.Dump());
            // this logical mexpr will be the last optimized one, mark it as the last task for this group
            if (ssp.GlobepsPruning) {
                Cost eps_bound = new Cost(EpsBound);
                ssp.getPTasks().push(
                    new O_EXPR(ssp,
                        LogMExpr,
                        true,
                        ContextID,
                        true,
                        eps_bound));
            } else
                ssp.getPTasks().push(
                    new O_EXPR(ssp, LogMExpr, true, ContextID, true));
        }
    } //perform

    
    public String toString() {
        String os = null;
        //        os.Format("E_GROUP group %d,", GrpID);
        //        String temp;
        //        temp.Format(" parent task %d", ParentTaskNo);
        //        os += temp;
        return os;
    } //Dump

} // E_GROUP