/* $Id: SSP.java,v 1.5 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.colombia;

import java.util.ArrayList;
import java.util.HashMap;

/**
   SEARCH SPACE 
   
   We borrow the term Search Space from AI, where it is a tool for solving a 
   problem  In query optimization the problem is to find the cheapest plan 
   for a given query subject to certain context (class Context).
   
   A Search Space typically consists of a collection of possible solutions
   to the problem and its subproblems. Dynamic Programming and
   Memoization are two approaches to using a Search Space to solve a problem.
   Both Dynamic Programming and Memoization partition the possible solutions 
   by logical equivalence. We will call each partition a Group.  Thus a Group
   contains a collection of logically equivalent expressions.
  
   In our setting of query optimization, logical equivalence is query
   equivalence.  Each Group corresponds to a temporary collection, or a
   subquery, in a computation of the main query.  If the main query is A
   join B join C, then there are GROUPs representing A join B, A join C,
   C, etc.
	
   In our approach to query optimization, each possible solution in the Search
   Space is represented compactly, by what we call a Multi-expression 
   (class MExpr).  A solution in the search space can also be represented in
   more detail by an expression (class Expr).
*/
public class SSP {
    private Tracer tracer;

    private int newGrpID;

    // SORT_AFTERS: sort possible moves in order of estimated cost
    public boolean SORT_AFTERS = false;

    //  _COSTS_ Prints the cost of each mexpr as it is costed, in the output window
    //  _TABLE_: prints one summary line for each optimization, using different epsilons
    //  _GEN_LOG: Used to control the generation of logical expressions when eps pruning is done.
    public boolean GEN_LOG = false;

    //  REUSE_SIB: An attempt to improve pattern matching by reusing one side of generated mexprs.
    public boolean REUSE_SIB = false;

    //  INFBOUND: When optimizing a group, ignore the initial upper bound; use infinity instead
    public boolean INFBOUND = false;

    public boolean Pruning = false; // global pruning flag

    // **************  include physcial mexpr in group or not *****************
    public boolean NO_PHYS_IN_GROUP = false;

    // IRPROP: off == Optimize each group for one property only
    // on == Optimize each group for all Interesting Relevant Properties
    public boolean IRPROP = false;

    // global epsilon pruning flag
    public boolean GlobepsPruning = false;

    //  FIRSTPLAN: trace when the first complete plan is costed
    public boolean FIRSTPLAN = false;

    ////GLOBAL_EPS is typically determined as a small percentage of 
    //// a cost found in a first pass optimization.
    ////Any subplan costing less than this is taken to be optimal.
    double GLOBAL_EPS = 0.5; // global epsilon value
    ////if GlobalepsPruning is not set, this value is 0
    ////otherwise, this value will be reset in main
    Cost GlobalEpsBound = new Cost(0);

    public boolean CuCardPruning = false; // global cucard pruning flag

    public static final int NEW_GRPID = -1;
    //     // use by CopyIn and MExpr::MExpr
    //     // means need to create a new group

    // XXX vpapad: turning global TaskNo into SSP member
    //Number of the current task.
    public int TaskNo = 0;

    /** pending tasks */
    private PendingTasks ptasks;

    private RuleSet ruleSet;

    // XXX vpapad moved from MultiWinner
    // XXX vpapad was: public  static std::vector< MultiWinner *> mc;
    private ArrayList mc;

    // XXX vpapad moved from Context
    //The vector of contexts, vc, implements sharing.  Each task which
    //creates a context  adds an entry to this vector.  
    private ArrayList vc;

    // Hash table used to detect duplicate multiexpressions
    public HashMap mexprs;

    // Catalog 
    private ICatalog catalog;

    public ICatalog getCatalog() {
        return catalog;
    }

    public SSP(RuleSet ruleSet, ICatalog catalog) {
        this.ruleSet = ruleSet;
        ruleSet.setSSP(this);
        this.catalog = catalog;
        Groups = new ArrayList();
        mc = new ArrayList();
        vc = new ArrayList();
        mexprs = new HashMap();
        ptasks = new PendingTasks();

        clear();
    }

    // return the next available grpID in SSP
    protected int getNewGrpID() {
        return (++newGrpID);
    }

    int getRulesetSize() {
        return ruleSet.size();
    }

    // free up memory
    public void clear() {
        newGrpID = -1;
        Groups.clear();
        mexprs.clear();
        mc.clear();
        vc.clear();
        ptasks.clear();
    }

    //     // Copy out all the logical expressions with no pruning.
    //     // By default, start with the top group (GrpID=0)
    //     std::vector < Expr *> * GetExprs(int GrpID=0);

    //     // Copy out all the physical plans with no pruning. 
    //     // By default, start with the top group (GrpID=0)
    //     std::vector < Expr *> * GetPlans(int GrpID=0);

    //     // A helper function for GetAllExprs and GetAllPlans
    //     // Return all the logical expressions or physical plans with no pruning.
    //     //	Logical expressions if forLogical == true
    //     //	Physical plans if forLogical == false
    //     std::vector<Expr*> * AllExprs(int GrpID, PhysicalProperty * PhysProp, boolean forLogical);

    //     std::vector< Group* > * GetGroups() { return &Groups; }
    //     // return the specific group
    public Group getGroup(int Gid) {
        return (Group) Groups.get(Gid);
    }

    public int getNumberOfGroups() {
        return Groups.size();
    }

    //     void  ShrinkGroup(int group_no);	//shrink the group marked completed
    //     void  Shrink();			//shrink the ssp

    //     boolean IsChanged(); // is the ssp changed?

    //     // Print groups, expressions, plans and costs
    //     void Space_Plans_Costs();

    //     // Call init_state for every group
    //     void AnotherRound();
    //     // XXX vpapad private:
    //     int	RootGID;
    //     int	InitGroupNum;

    //     //Collection of Groups, indexed by GRP_ID
    private ArrayList Groups;

    // void Shrink()
    // {
    // 	for(int i=InitGroupNum; i<Groups.size();i++)	
    //             ShrinkGroup(i);
    // }

    // void ShrinkGroup(int group_no)
    // {
    //     Group* Group;
    //     MExpr * mexpr;
    //     MExpr * p;
    //     MExpr * prev;
    //     int DeleteCount=0;

    // 	SET_TRACE Trace(true);

    // 	Group = Groups[group_no];

    // 	if(! Group.is_optimized() && ! Group.is_explored()) return ;   // may be pruned

    // 	PTRACE("Shrinking group %d,", group_no);

    // 	// Shrink the logical mexpr
    // 	// init the rule mark of the first mexpr to 0, means all rules are allowed
    // 	mexpr =  Group.GetFirstLogMExpr();
    // 	mexpr.clear_rule_mask();

    // 	// delete all the mexpr except the first initial one
    // 	mexpr = mexpr.GetNextMExpr();

    // 	while(mexpr != null)
    // 	{
    // 		// maintain the hash link
    // 		// find my self in the appropriate hash bucket
    // 		ub4 hashval = mexpr.hash();
    // 		for (p = HashTbl[ hashval ], prev = null;
    // 		p != mexpr;  
    //     				prev = p, p = p . GetNextHash()) ;

    // 					assert(p==mexpr);
    // 					// link prev's next hash to next 
    // 					if(prev) 
    // 						prev.SetNextHash(mexpr.GetNextHash());
    // 					else
    // 						// the mexpr is the first in the bucket
    // 						HashTbl[ hashval ] = mexpr.GetNextHash();

    // 					p = mexpr;
    // 					mexpr = mexpr.GetNextMExpr();

    // 					delete p; 
    // 					DeleteCount ++;
    // 	}

    // 	mexpr =  Group.GetFirstLogMExpr();
    // 	mexpr.SetNextMExpr(null);
    // 	// update the lastlogmexpr = firstlogmexpr;
    // 	Group.SetLastLogMExpr(mexpr);

    // 	// Shrink the physcal mexpr
    // 	mexpr =  Group.GetFirstPhysMExpr();

    // 	while(mexpr != null)
    // 	{
    // 		p = mexpr;
    // 		mexpr = mexpr.GetNextMExpr();

    // 		delete p; 
    // 		DeleteCount ++;
    // 	}

    // 	mexpr =  Group.GetFirstPhysMExpr();
    // 	mexpr.SetNextMExpr(null);
    // 	// update the lastlogmexpr = firstlogmexpr;
    // 	Group.SetLastPhysMExpr(mexpr);

    // 	Group.set_changed(true);
    // 	Group.set_exploring(false);

    // 	PTRACE("Deleted %d mexpr!\r\n", DeleteCount);
    // }

    // String Dump()
    // {
    //     String os;
    //     Group* Group;

    // 	os.Format("%s%d%s","RootGID:" , RootGID , "\r\n");

    // 	for(int i=0; i< Groups.size();i++)
    // 	{
    // 		Group = Groups[i];
    // 		os += Group.Dump();
    // 		Group.set_changed(false);
    // 	}

    // 	return os;
    // }

    /** Return registered multiexpression that is equal (but <em>not identical</em>)
     *  to <code>MExpr</code>, register <code>MExpr</code> and return null 
     *  if there is none.
     */
    MExpr FindDup(MExpr mexpr) {
        MExpr dup = (MExpr) mexprs.get(mexpr);
        if (mexpr.equals(dup)) {
            tracer.duplicateMExprFound(mexpr);
            return dup;
        }

        mexprs.put(mexpr, mexpr);
        return null;
    }

    //When a duplicate is found in two groups they should be merged into
    // the same group.  We always merge bigger group_no group to smaller one.    

    // // Merge already existing group is problematic!! What about parents.  
    // // QuanW 5/ 2000 
    int MergeGroups(int group_no1, int group_no2) {

        //XXX vpapad: We can't handle group merging yet;

        assert !ruleSet.isUnique() : "Duplicates detected in unique rule set";
        int ToGid = group_no1;
        int FromGid = group_no2;

        // always merge bigger group_no group to smaller one.
        if (group_no1 > group_no2) {
            ToGid = group_no2;
            FromGid = group_no1;
        }

        return ToGid;
    }

    // Convert the Expr into a Mexpr. 
    // If Mexpr is not already in the search space, then copy Mexpr into the 
    // search space and return the new Mexpr.  
    // If Mexpr is already in the search space, then either throw it away or
    // merge groups.
    // GrpID is the ID of the group where Mexpr will be put.  If GrpID is 
    // NEW_GRPID(-1), make a new group with that ID.
    // XXX vpapad: GrpId was passed by reference

    public MExpr copyIn(Expr Expr, int GrpID) {
        return copyIn(Expr, GrpID, false);
    }

    public MExpr copyIn(Expr Expr, int GrpID, boolean returnDuplicate) {
        Group Group;

        // create the M_Expr which will reside in the group
        MExpr MExpr = new MExpr(Expr, GrpID, this);
        int PrevGID;

        // find duplicate.  Done only for logical, not physical, expressions.
        if (MExpr.getOp().is_logical()) {
            MExpr DupMExpr = FindDup(MExpr);
            if (DupMExpr != null) { // not null ,there is a duplicate
                //PTRACE0("duplicate mexpr : " + MExpr.LightDump());

                // the duplicate is in the group the expr wanted to copyin
                if (GrpID == DupMExpr.getGrpID()) {
                    if (returnDuplicate)
                        return DupMExpr;
                    else
                        return null;
                }

                // If the Mexpr is supposed to be in a new group, set the group id 
                if (GrpID == NEW_GRPID) {
                    // because the NewGrpID increases when constructing 
                    // an MExpr with NEW_GRPID, we need to decrease it
                    newGrpID--;

                    if (returnDuplicate)
                        return DupMExpr;
                    else
                        return null;

                } else {
                    // otherwise, i.e., GrpID != DupMExpr.GrpID
                    // need do the merge
                    MergeGroups(GrpID, DupMExpr.getGrpID());
                    if (returnDuplicate)
                        return DupMExpr;
                    else
                        return null;
                }
            } // if(DupMExpr != null)
        } //If the expression is logical

        // no duplicate found
        if (GrpID == NEW_GRPID) {
            // create a new group
            Group = new Group(MExpr, this);

            // insert the new group into ssp
            GrpID = Group.getGroupID();

            if (GrpID >= Groups.size()) {
                Groups.ensureCapacity(GrpID + 1);
                // Add nulls for parent groups that have not 
                // been created yet
                for (int i = Groups.size(); i < GrpID; i++) {
                    Groups.add(null);
                }
                Groups.add(Group);
            } else {
                Groups.set(GrpID, Group);
            }

            PrevGID = GrpID;

            if (IRPROP) {
                // For the topmost group and for the groups containing the item operator and constant
                // operator, set the only physical property as any and bound INF
                if (GrpID == 0 || ((MExpr.getOp()).is_item())) {
                    MultiWinner MWin = new MultiWinner(1);
                    mc.ensureCapacity(GrpID + 1);
                    mc.add(GrpID, MWin);
                } else {
                    // XXX vpapad: seriously messed up
                    // get the relevant attributes from the schema for this group
                    Strings tmpKeySet = Group.getLogProp().GetAttrNames();
                    int ksize = tmpKeySet.size();

                    MultiWinner MWin = new MultiWinner(ksize + 1);

                    for (int i = 1; i < ksize + 1; i++) {
                        Strings MKEYS = new Strings();
                        MKEYS.add(tmpKeySet.get(i - 1));
                        PhysicalProperty Prop =
                            new PhysicalProperty(
                                new Order(Order.Kind.SORTED, MKEYS));
                        //Prop.KeyOrder.Add(new Order(ascending));
                        MWin.SetPhysProp(i, Prop);
                    }
                    mc.ensureCapacity(GrpID + 1);
                    mc.add(GrpID, MWin);
                }
            }
        } else {
            Group = getGroup(GrpID);

            // include the new MEXPR
            Group.newMExpr(MExpr);
        }
        // set the flag
        Group.setChanged(true);

        return MExpr;
    } // CopyIn

    /** Return the optimal expression that satisfies this property, 
     * null otherwise */
    public Expr CopyOut(Group group, PhysicalProperty PhysProp, HashMap seen) {
        Winner ThisWinner;

        MExpr WinnerMExpr;
        Op WinnerOp;

        //special case for item groups
        if (group.getFirstLogMExpr().getOp().is_item()) {
            if (IRPROP) {
                WinnerMExpr = getMc(group.getGroupID()).getBPlan(PhysProp);
                if (WinnerMExpr == null) {
                    return null;
                }
            } else {
                ThisWinner = group.getWinner(PhysProp);
                if (ThisWinner == null) {
                    return null;
                }

                assert(ThisWinner.getDone());
                WinnerMExpr = ThisWinner.getMPlan();
            }
            WinnerOp = WinnerMExpr.getOp();

            Expr[] inputs = new Expr[WinnerMExpr.getArity()];
            for (int i = 0; i < WinnerMExpr.getArity(); i++) {
                Group inputGroup = WinnerMExpr.getInput(i);
                if (seen.containsKey(inputGroup))
                    inputs[i] = (Expr) seen.get(inputGroup);
                else
                    inputs[i] =
                        CopyOut(
                            WinnerMExpr.getInput(i),
                            PhysicalProperty.ANY,
                            seen);
            }
            Expr e = new Expr(WinnerOp, inputs);
            seen.put(group, e);
            return e;
        } else { // normal case
            if (!IRPROP) {
                ThisWinner = group.getWinner(PhysProp);

                if (ThisWinner == null) {
                    return null;
                }

                assert(ThisWinner.getDone());
                WinnerMExpr = ThisWinner.getMPlan();
                if (WinnerMExpr == null) {
                    return null;
                }

            } else {
                WinnerMExpr = getMc(group.getGroupID()).getBPlan(PhysProp);
            }

            WinnerOp = WinnerMExpr.getOp();

            int Arity = WinnerOp.getArity();
            Expr[] inputs = new Expr[Arity];
            for (int i = 0; i < Arity; i++) {
                Group inputGroup = WinnerMExpr.getInput(i);

                if (seen.containsKey(inputGroup)) {
                    inputs[i] = (Expr) seen.get(inputGroup);
                    continue;
                }
                
                PhysicalProperty[] properties =
                    ((PhysicalOp) WinnerOp).inputReqdProp(
                        PhysProp,
                        inputGroup.getLogProp(),
                        i);

                if (properties == null)
                    return null;

                PhysicalProperty ReqProp;
                if (properties.length > 0)
                    ReqProp = properties[0];
                else
                    ReqProp = PhysicalProperty.ANY;

                inputs[i] = CopyOut(inputGroup, ReqProp, seen);
            }
            Expr e = new Expr(WinnerOp, inputs);
            seen.put(group, e);
            return e;
        }
    }

    /** Fully extract the last logical expression for this group */
    public Expr extractLastLogicalExpression(Group group) {
        // XXX vpapad: does not handle shared subexpressions
        MExpr mexpr = group.getLastLogMExpr();
        Op op = mexpr.getOp();
        Expr[] inputs = new Expr[mexpr.getArity()];
        for (int i = 0; i < mexpr.getArity(); i++) {
            inputs[i] = extractLastLogicalExpression(mexpr.getInput(i));
        }
        return new Expr(op, inputs);
    }

    // 	// Return all the logical expression with no pruning. 
    // 	vector<Expr*> * GetExprs(int GrpID) {
    // 		return AllExprs(GrpID, 0, true);
    // 	}

    // 	// Return all the physical plans with no pruning. 
    // 	vector<Expr*> * GetPlans(int GrpID){
    // 		return AllExprs(GrpID, new PhysicalProperty(new Order(any)), false);
    // 	}

    //    // Return all the logical expressions or physical plans with no pruning.
    //    //	Logical expressions if forLogical == true
    //    //	Physical plans if forLogical == false
    //    ArrayList AllExprs(
    //        Group group,
    //        PhysicalProperty PhysProp,
    //        boolean forLogical) {
    //        ArrayList Exprs = new ArrayList();
    //        Group ThisGroup = group;
    //        MExpr MExpr;
    //
    //        if (forLogical)
    //            MExpr = ThisGroup.GetFirstLogMExpr();
    //        else
    //            MExpr = ThisGroup.GetFirstPhysMExpr();
    //
    //        while (MExpr != null) { // Traverse the MExpr list
    //            boolean possible;
    //            Group SubGroup;
    //            PhysicalProperty SubProp;
    //            Op op = MExpr.getOp();
    //
    //            if (!forLogical)
    //                ((PhysicalOp) op).SetGrpID(GrpID);
    //            // Set the GrpID for this operator (see Op for GrpID)
    //
    //            ArrayList SubExprs = null;
    //            ArrayList SubExprs1 = null;
    //
    //            // If the MExpr has no input
    //            if (MExpr.getArity() == 0)
    //                Exprs.add(new Expr(op.copy()));
    //
    //            // Get the expressions from the first input group,
    //            // if the M_Expr has at least one input group
    //            // Derive the expressions, when the M_Expr has one input group
    //            else {
    //                SubGroup = MExpr.GetInput(0);
    //                if (forLogical) // For logical expressions
    //                    SubExprs = Ssp.AllExprs(SubGroup.GetGroupID(), 0, true);
    //                else { // For physical plans
    //                    SubProp =
    //                        ((PhysicalOp *) op).InputReqdProp(
    //                            PhysProp,
    //                            SubGroup.getLogProp(),
    //                            0,
    //                            possible);
    //                    if (possible)
    //                        SubExprs =
    //                            Ssp.AllExprs(SubGroup, SubProp, false);
    //                }
    //            }
    //
    //            // Derive the expressions, when the M_Expr has one input group
    //            if (MExpr.getArity() == 1
    //                && SubExprs) { // If it has one input groups
    //                for (int i = 0; i < SubExprs.size(); i++)
    //                    Exprs.push_back(new Expr(op.Copy(), (* SubExprs)[i]));
    //            }
    //
    //            // Derive the expressions , when the M_Expr has two input group
    //            if (MExpr.getArity() == 2 && SubExprs) {
    //                SubGroup = Ssp.GetGroup(MExpr.GetInput(1));
    //                if (forLogical)
    //                    SubExprs1 = Ssp.AllExprs(SubGroup.GetGroupID(), 0, true);
    //                else {
    //                    SubProp =
    //                        ((PhysicalOp *) op).InputReqdProp(
    //                            PhysProp,
    //                            SubGroup.getLogProp(),
    //                            1,
    //                            possible);
    //                    if (possible)
    //                        SubExprs1 =
    //                            Ssp.AllExprs(SubGroup.GetGroupID(), SubProp, false);
    //                }
    //                if (SubExprs && SubExprs1) {
    //                    for (int i = 0; i < SubExprs.size(); i++)
    //                        for (int j = 0; j < SubExprs1.size(); j++)
    //                            Exprs.push_back(
    //                                new Expr(
    //                                    op.Copy(),
    //                                    (* SubExprs)[i],
    //                                    (* SubExprs1)[j]));
    //                }
    //            }
    //            MExpr = MExpr.GetNextMExpr();
    //        }
    //        return Exprs;
    //    } //AllExprs()

    public void optimize(Expr expr) {
        tracer.startingOptimization();

        copyIn(expr, NEW_GRPID);

        // Compute the global epsilon bound if we have to
        getGlobalEpsBound(expr);

        if (FIRSTPLAN)
            getGroup(0).setfirstplan(false);

        //Create initial context, with no requested properties, infinite upper bound,
        // zero lower bound, not yet done.  Later this may be specified by user.
        Context InitCont =
            new Context(
                new PhysicalProperty(Order.newAny()),
                new Cost(-1),
                false);
        //Make this the first context
        vc.add(InitCont);

        // XXX vpapad: RootGID was passed by ref. into CopyIn
        // For now we'll just assume it's 0
        int RootGID = 0;

        // start optimization with root group, 0th context, parent task of zero.  
        if (GlobepsPruning) {
            Cost eps_bound = new Cost(GlobalEpsBound);
            ptasks.push(
                new O_GROUP(
                    this,
                    (Group) Groups.get(RootGID),
                    0,
                    true,
                    eps_bound));
        } else
            ptasks.push(new O_GROUP(this, (Group) Groups.get(RootGID), 0));
        // main loop of optimization
        // while there are tasks undone, do one
        while (!ptasks.empty()) {
            TaskNo++;

            Task NextTask = ptasks.pop();
            tracer.performingTask(NextTask);
            NextTask.perform();
        }
        tracer.endingOptimization();
    }

    public Rule getRule(int ruleID) {
        return (Rule) ruleSet.get(ruleID);
    }

    public void addTask(Task t) {
        getTracer().addingTask(t);
        ptasks.push(t);
    }

    /**
     * Returns the multiwinners list.
     * @return ArrayList
     */
    public MultiWinner getMc(int i) {
        return (MultiWinner) mc.get(i);
    }

    public Context getVc(int contextID) {
        return (Context) vc.get(contextID);
    }

    public void addVc(Context c) {
        vc.add(c);
    }

    public int getVcSize() {
        return vc.size();
    }

    /**
     * Returns the current tracer object.
     * @return Tracer
     */
    public Tracer getTracer() {
        return tracer;
    }
    /**
     * Sets the tracer object.
     * @param tracer The new tracer 
     */
    public void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    //   Obtain the cost for globalEpsPrunning
    void getGlobalEpsBound(Expr expr) {
        PhysicalProperty PhysProp;

        // if GlobepsPruning, run optimizer without globepsPruning
        // to get the heuristic cost
        if (GlobepsPruning) {
            GlobepsPruning = false;

            optimize(expr);
            PhysProp = getVc(0).getPhysProp();
            Cost HeuristicCost = new Cost(0);
            HeuristicCost = (getGroup(0).getWinner(PhysProp).getCost());
            assert(getGroup(0).getWinner(PhysProp).getDone());
            GlobalEpsBound = HeuristicCost.times(GLOBAL_EPS);
            clear();
            GlobepsPruning = true;
        }
    }
}
