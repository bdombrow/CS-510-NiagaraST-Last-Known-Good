package niagara.optimizer.colombia;

import java.util.ArrayList;

/*
 
	 ========================================================
	 O_INPUTS - Task to Optimize inputs 
	 ========================================================
  	This task is rather misnamed.  It:
		1) Determines whether the (physical) MExpr satisfies the task's context
		2) As part of (1), it may optimize/cost some inputs
		3) It may adjust the current context and the current winner.
		It may use bounds, primarily upper bounds, in its work.
      
		  Member data InputNo, initially 0, indicates which input has been 
		  costed.  This task is unique in that it does not terminate after 
		  scheduling other tasks.  If the current input needs to be optimized, 
		  it first pushes itself onto the stack, then it schedules 
		  the optimization of the current input.  If and when inputs are all
		  costed, it calculates the cost of the entire physical expression.
*/
public class O_INPUTS extends Task {

    private MExpr MExpr; // expression whose inputs we are optimizing
    int arity;
    int InputNo; // input currently being or about to be optimized, initially 0
    int PrevInputNo; // keep track of the previous optimized input no
    Cost LocalCost; // the local cost of the mexpr
    boolean Last; // if this task is the last task for the group
    Cost EpsBound;
    // if global eps pruning is on, this is the eps bound for eps pruning
    // else it is zero
    int ContNo; // keep track of number of contexts 

    //Costs and properties of input winners and groups.  Computed incrementally
    // by this method.
    Cost[] InputCost;
    LogicalProperty[] InputLogProp;

    public O_INPUTS(MExpr MExpr, int ContextID) {
        this(MExpr, ContextID, false, null, 0);
    }

    public O_INPUTS(MExpr MExpr, int ContextID, boolean last) {
        this(MExpr, ContextID, last, null, 0);
    }

    public O_INPUTS(MExpr MExpr, int ContextID, boolean last, Cost bound) {
        this(MExpr, ContextID, last, bound, 0);
    }

    public O_INPUTS(
        MExpr MExpr,
        int ContextID,
        boolean last,
        Cost bound,
        int ContNo) {
        super(MExpr.getGroup().getSSP(), ContextID);
        this.MExpr = MExpr;
        InputNo = -1;
        Last = last;
        PrevInputNo = -1;
        EpsBound = bound;
        ContNo = ContNo;

        assert(MExpr.getOp().is_physical() || MExpr.getOp().is_item());
        //We can only calculate cost for physical operators

        //Cache local properties
        Op Op = (PhysicalOp) (MExpr.getOp()); // the op of the expr
        arity = Op.getArity(); // cache arity of mexpr

        // create the arrays of input costs and logical properties
        if (arity > 0) {
            InputCost = new Cost[arity];
            InputLogProp = new LogicalProperty[arity];
        }

    }

    /**
    perform
    
      NOTATION
      InputCost[]: Contains actual (or lower bound) costs of optimal inputs to G.
      CostSoFar: LocalCost + sum of all InputCost entries. 
      G: the group being optimized.  
      IG: various inputs to expressions in G.
      SLB: (Special Lower Bound) The Lower Bound of G, derived with fetch and cucard 
      We use this term instead of Lower Bound since we will use other lower bounds.
      
        There are still three flags: Pruning (also called Group Pruning), CuCardPruning and GlobepsPruning, 
        with new meanings.  We plan to run benchmarks in four cases:
        
          1. Starburst - generate all expressions [!Pruning && !CuCardPruning]
          2. Group Pruning - aggressively check limits at all times [Pruning && !CuCardPruning]
          aggressively check means if (CostSoFar >= upper bound of context in G) then terminate.
          3. Lower Bound Pruning - if there is no winner, then use IG's SLB in InputCost[]. [CuCardPruning].
          This case assumes that the Pruning flag is on, i.e. the code forces Pruning to be true
          if CuCardPruning is true.  The SLB may involve cucard, fetch, copy, etc in the lower bound.
          4. Global Epsilon Pruning [GlobepsPruning].  If a plan costs <= GLOBAL_EPS, it is a winner for G.
          
            PSEUDOCODE
            
              On the first (and no other) execution, the code must initialize some O_INPUTS members.
              The idea here is to get a quick lower bound for the cost of the inputs.
              The only nontrivial member is InputCost; here is how to initialize it:
              //Initial values of InputCost are zero in the Starburst case
              For each input group IG
              If (Starburst case)
              InputCost is zero
              continue
              Determine property required of search in IG
              If no such property, terminate this task.
              call search_circle on IG with that property, infinite cost.
              
                If case (1), no possibility of satisfying the context
                terminate this task
                If search_circle returns a non-null Winner from IG, case (2)
                InputCost[IG] = cost of that winner
                else if (!CuCardPruning) //Group Pruning case (since Starburst not relevant here)
                InputCost[IG] = 0
                //remainder is Lower Bound Pruning case
                else if there has been no previous search for ReqdProp
                InputCost[IG] = SLB 
                else if there has been a previous search for ReqdProp
                InputCost[IG] = max(cost of winner, IG's SLB) //This is a lower bound for IG
                else
                error - previous cost failed because of property
                
                  //The rest of the code should be executed on every execution of the task.
                  
                    If (Pruning && CostSoFar >= upper bound) terminate.
                    
                      if (arity==0 and required property can not be satisfied)
                      terminate this task
                      
                        //Calculate cost of remaining inputs
                        For each remaining (from InputNo to arity) input group IG
                        Call search_circle()
                        If Starburst case and case (1)
                        error
                        else if case (1)
                        terminate this task
                        else If there is a non-null Winner in IG, case (2)
                        store its cost in InputCost
                        if (Pruning && CostSoFar exceeds G's context's upper bound) terminate task
                        else if (we did not just return from O_GROUP on IG)
                        //optimize this input; seek a winner for it
                        push this task
                        push O_GROUP for IG, using current context's cost minus CostSoFar plus InputCost[InputNo]
                        terminate this task
                        else // we just returned from O_GROUP on IG
                        Trace: This is an impossible plan
                        terminate this task
                        InputNo++;
                        endFor //calculate the cost of remaining inputs 
                        
                          //Now all inputs have been optimized
                          
                            if (CostSoFar >  G's context's upper bound)
                            terminate this task
                            
                              //Now we know current expression satisfies current context.
                              
                                if(GlobepsPruning && CostSoFar <= GLOBAL_EPS)
                                Make current mexpression a done winner for G
                                mark the current context as done 
                                terminate this task
                                
                                  //Now we consider the possible states of the relevant winner in G
                                  
                                    Search the winner's circle in G for the current task's physical property
                                    If there is no such winner
                                    error - the search should have initialized a winner
                                    else If winner is done 
                                    error - we are in the midst of a search, not yet done
                                    else If (winner is non-null and CostSoFar >= cost of this winner) || winner is null
                                    Replace existing winner with current mexpression and its cost, don't change done flag
                                    Update the upper bound of the current context
    */

    public void perform() {
        //        PTRACE2(
        //            "O_INPUT performing Input %d, expr: %s",
        //            InputNo,
        //            (const char *) MExpr.Dump());
        //        PTRACE2(
        //            "Context ID: %d , %s",
        //            ContextID,
        //            (const char *) Context : : vc[ContextID].Dump());
        //        PTRACE("Last flag is %d", Last);
        //Cache local properties of G and the expression being optimized

        assert MExpr.getOp().is_physical() 
            : "We can only optimize the inputs of physical expressions";
        PhysicalOp Op = (PhysicalOp) MExpr.getOp(); //the op of the expr

        Group LocalGroup = MExpr.getGroup();
        //Group of the MExpr

        PhysicalProperty LocalReqdProp = ((Context) ssp.vc.get(ContextID)).getPhysProp();
        //What prop is required
        Cost LocalUB = ((Context) ssp.vc.get(ContextID)).getUpperBd();

        //if global eps pruning happened, terminate this task
        if (ssp.GlobepsPruning && ((Context) ssp.vc.get(ContextID)).is_done()) {
            //PTRACE("%s", "Task terminated due to global eps pruning");
            if (Last)
                // this's the last task for the group, so mark the group with completed optimizing
                LocalGroup.set_optimized(true);
            return;
        }

        Group IG;
        int input; //index over input groups
        Cost CostSoFar = new Cost(0);

        Winner LocalWinner = LocalGroup.getWinner(LocalReqdProp);
        //Winner in G
        Cost Zero = new Cost(0);

        //On the first (and no other) execution, code must initialize some O_INPUTS members.
        //The only nontrivial member is InputCost.
        if (InputNo == -1) {
            // init inputLogProp
            for (input = 0; input < arity; input++) {
                Group InputGroup = MExpr.getInput(input);
                //Group of current input
                InputLogProp[input] = InputGroup.getLogProp();
            }

            // get the localcost of the mexpr being optimized in G
            LocalCost =
                Op.FindLocalCost(
                    ssp.getCatalog(),
                    LocalGroup.getLogProp(),
                    InputLogProp);

            //For each input group IG
            for (input = 0; input < arity; input++) {
                // Initial values of InputCost are zero in the Starburst (no Pruning) case
                if (!ssp.Pruning) {
                    if (input == 0)
                        //                        PTRACE(
                        //                            "%s",
                        //                            "Not pruning so all InputCost elements are set to zero");
                        assert(!ssp.CuCardPruning);
                    InputCost[input] = Zero;
                    continue;
                }

                IG = MExpr.getInput(input); //Group of current input

                PhysicalProperty[] properties;
                PhysicalProperty ReqProp = null;
                if (Op.is_physical()) {
                    // Determine property required of that input
                    properties =
                        ((PhysicalOp) Op).InputReqdProp(
                            LocalReqdProp,
                            InputLogProp[input],
                            input);
                    // if not possible, means no such input prop can satisfied
                    if (properties == null) {
                        //PTRACE("Impossible search: Bad input %d", input);
                        TerminateThisTask(LocalWinner);
                        return;
                    } else {
                        if (properties.length == 0) {
                            // Any property will do
                            ReqProp = PhysicalProperty.ANY;
                        } else {
                            // XXX vpapad: The new interface for InputReqdProp
                            // allows an operator to specify that any of a number
                            // of properties will do, for now we'll just choose
                            // the first one.
                            ReqProp = properties[0];
                        }
                    }
                } else
                    ReqProp = PhysicalProperty.ANY;

                //call search_circle on IG with that property, infinite cost.
                Cost INFCost = new Cost(-1);

                Context IGContext = new Context(ReqProp, INFCost, false);
                Group.SearchResults sr = IG.search_circle(IGContext);
                //                PTRACE2(
                //                    "search_circle(): more search %s needed, return value is %s",
                //                    moreSearch ? "" : "not",
                //                    SCReturn ? "true" : "false");

                //If case (1), impossible, then terminate this task
                if (sr.isImpossible()) {
                    //                    PTRACE("Impossible search: Bad input %d", input);
                    TerminateThisTask(LocalWinner);
                    return;
                }
                //If search_circle returns a non-null Winner from InputGroup, case (2)
                //InputCost[InputGroup] = cost of that winner
                else if (sr.haveWinner()) {
                    InputCost[input] = IG.getWinner(ReqProp).getCost();
                    assert IG.getWinner(ReqProp).getDone() : "XXX @$@#$@#$";
                }
                //else if (!CuCardPruning) //Group Pruning case (since Starburst not relevant here)
                //InputCost[IG] = 0
                else if (!ssp.CuCardPruning)
                    InputCost[input] = Zero;
                //remainder applies only in CuCardPruning case
                else
                    InputCost[input] = IG.getLowerBd();

                IGContext = null;
            } // initialize some O_INPUTS members

            InputNo++;
            // Ensure that previous code will not be executed again; begin with Input 0
        }

        //If Global Pruning and cost so far is greater than upper bound for this context, then terminate
        CostSoFar.finalCost(LocalCost, InputCost);
        if (ssp.Pruning && CostSoFar.greaterThanEqual(LocalUB)) {
            //            PTRACE2(
            //                "Expr LowerBd %s, exceed Cond UpperBd %s,Pruning applied!",
            //                (const char *) CostSoFar.Dump(),
            //                (const char *) LocalUB.Dump());

            TerminateThisTask(LocalWinner);
            return;
        }

        //Calculate the cost of remaining inputs
        for (input = InputNo; input < arity; input++) {
            //set up local variables
            IG = MExpr.getInput(input); //Group of current input
            //generate appropriate property for search of IG
            PhysicalProperty[] properties = null;
            PhysicalProperty ReqProp = null;
            if (Op.is_physical()) {
                // Determine property required of that input
                properties =
                    ((PhysicalOp) Op).InputReqdProp(
                        LocalReqdProp,
                        InputLogProp[input],
                        input);

                if (ssp.Pruning)
                    assert(properties != null) : " XXX @#$@#$@#$#";
                // should be possible since in the first pass, we checked it

                if (properties == null) {
                    ReqProp = null;
                    TerminateThisTask(LocalWinner);
                    return;
                } else if (properties.length > 0) {
                    ReqProp = (PhysicalProperty) properties[0];
                } else
                    ReqProp = PhysicalProperty.ANY;
            } else
                ReqProp = PhysicalProperty.ANY;

            Cost INFCost = new Cost(-1);

            //call search_circle on IG with that property, infinite cost.
            Context IGContext = new Context(ReqProp, INFCost, false);
            Group.SearchResults sr = IG.search_circle(IGContext);

            //If case (1), impossible so terminate
            if (sr.isImpossible()) {
                //                PTRACE("Impossible search: Bad input %d", input);
                IGContext = null;
                TerminateThisTask(LocalWinner);
                return;
            }

            //else if Case 2: There is a winner with nonzero plan, in current input
            else if (sr.haveWinner()) {
                //PTRACE("Found Winner for Input : %d", input);
                Winner Winner = IG.getWinner(ReqProp);
                assert Winner.getDone() : "XXX ^$$%^$%^$%^$%^%";

                //store its cost in InputCost[]
                InputCost[input] = Winner.getCost();

                CostSoFar.finalCost(LocalCost, InputCost);
                //if (Pruning && CostSoFar >= upper bound) terminate this task
                if (ssp.Pruning && CostSoFar.greaterThanEqual(LocalUB)) {
                    //                    PTRACE2(
                    //                        "Expr LowerBd %s, exceed Cond UpperBd %s,Pruning applied!",
                    //                        (const char *) CostSoFar.Dump(),
                    //                        (const char *) LocalUB.Dump());
                    //                    PTRACE("This happened at group %d ", IGNo);

                    IGContext = null;
                    TerminateThisTask(LocalWinner);
                    return;
                }
                IGContext = null;
            }

            //Remaining cases are (3) and (4)
            else if (input != PrevInputNo) {
                // no winner, and we did not just return from O_GROUP
                //                PTRACE("No Winner for Input : %d", input);

                //Adjust PrevInputNo and InputNo to track progress after returning from pushes
                PrevInputNo = input;
                InputNo = input;

                //push this task
                ssp.addTask(this);
                //PTRACE("push myself, %s", "O_INPUT");

                //Build a context for the input group task
                //First calculate the upper bound for search of input group.
                //Upper bounds are irrelevant unless we are pruning
                Cost InputBd = new Cost(LocalUB);
                //Start with upper bound of G's context
                if (ssp.Pruning) {
                    //                    PTRACE0("LocalCost is " + LocalCost.Dump());
                    CostSoFar.finalCost(LocalCost, InputCost);
                    InputBd.subtract(CostSoFar); //Subtract CostSoFar
                    InputBd.add(InputCost[input]);
                    //Add IG's contribution to CostSoFar
                }

                PhysicalProperty InputProp = new PhysicalProperty(ReqProp);
                // update the bound in multiwinner to InputBd
                Context InputContext = new Context(InputProp, InputBd, false);
                ssp.vc.add(InputContext);
                //Push O_GROUP
                int ContID = ssp.vc.size() - 1;
                //                PTRACE2(
                //                    "push O_GROUP %d, %s",
                //                    IGNo,
                //                    (const char *) Context : : vc[ContID].Dump());

                if (ssp.GlobepsPruning) {
                    Cost eps_bound;
                    if (EpsBound.greaterThan(LocalCost)) {
                        eps_bound = new Cost(EpsBound);
                        // calculate the cost, the lower nodes should have lower eps bound
                        eps_bound.subtract(LocalCost);
                    } else
                        eps_bound = new Cost(0);
                    if (arity > 0)
                        eps_bound.divide(arity);
                    ssp.addTask(
                        new O_GROUP(
                            ssp,
                            MExpr.getInput(input),
                            ContID,
                            true,
                            eps_bound));
                } else
                    ssp.addTask(
                        new O_GROUP(ssp, MExpr.getInput(input), ContID));

                return;
            } else { // We just returned from O_GROUP on IG
                // impossible plan for this context
                //                PTRACE(
                //                    "impossible plan since no winner possible at input %d",
                //                    InputNo);

                TerminateThisTask(LocalWinner);
                return;
            }
        } //Calculate the cost of remaining inputs

        // If arity is zero, we need to ensure that this expression can
        // satisfy this required property.
        if (arity == 0
            && !LocalReqdProp.getOrder().isAny()
            && Op.is_physical()) {
            PhysicalProperty OutputPhysProp = ((PhysicalOp) Op).FindPhysProp(PhysicalOp.NO_INPUTS);
            if (!(LocalReqdProp == OutputPhysProp)) {
                //                PTRACE2(
                //                    "physical epxr: %s does not satisfy required phys_prop: %s",
                //                    (const char *) MExpr.Dump(),
                //                    (const char *) LocalReqdProp.Dump());
                OutputPhysProp = null;

                TerminateThisTask(LocalWinner);
                return;
            }
        }

        //All inputs have been been optimized, so compute cost of the expression being optimized.

        if (ssp.FIRSTPLAN) {
            //If we are in the root group and no plan in it has been costed
            if (!(MExpr.getGrpID() == 0) && !(LocalGroup.getfirstplan())) {
                // OUTPUT("First Plan is costed at task %d\r\n", TaskNo);
                LocalGroup.setfirstplan(true);
            }
        }
        CostSoFar.finalCost(LocalCost, InputCost);
        // PTRACE0("Expression's Cost is " + CostSoFar.Dump());
        // OUTPUT("COSTED %s  ", MExpr - > Dump());
        // OUTPUT("%s\r\n", CostSoFar.Dump());
        if (ssp.GlobepsPruning) {
            //PTRACE0("Current Epsilon Bound is " + EpsBound.Dump());
            //If global epsilon pruning is on, we may have an easy winner
            if (EpsBound.greaterThanEqual(CostSoFar)) {
                //we are done with this search, we have a final winner
                //                PTRACE(
                //                    "Global Epsilon Pruning fired, %s",
                //                    "got a final winner for this context");

                Cost WinCost = new Cost(CostSoFar);
                //Cost WinCost(CostSoFar);
                LocalGroup.newWinner(LocalReqdProp, MExpr, WinCost, true);
                // update the upperbound of the current context
                 ((Context) ssp.vc.get(ContextID)).setUpperBound(CostSoFar);
                ((Context) ssp.vc.get(ContextID)).done();
                TerminateThisTask(LocalWinner);
                return;
            }
        }

        // if halt, halt optimize the group when either number of plans since the
        // last winner >= HaltGrpSize*EstiGrpSize or the improvement in last HaltWinSize
        // winners is <= HaltImpr. This only works for EQJOIN

        //Check that winner satisfies current context 
        if (ssp.Pruning && CostSoFar.greaterThanEqual(LocalUB)) {
            //            PTRACE2(
            //                "total cost too expensive: totalcost %s >= upperbd %s",
            //                (const char *) CostSoFar.Dump(),
            //                (const char *) LocalUB.Dump());

            TerminateThisTask(LocalWinner);
            return;
        }

        // compare cost to current winner for this context
        // update the winner and upperbound accordingly
        if (LocalWinner.getMPlan() != null
            && //If there is already a non-null local winner
        CostSoFar
            .greaterThanEqual(
                LocalWinner
                    .getCost()) //and current expression is more expensive
        ) {
            //Leave the non-null local winner alone
            TerminateThisTask(LocalWinner);
            return;
        }
        else {
            //The expression being optimized is a new winner
            Cost WinCost = new Cost(CostSoFar);
            LocalGroup.newWinner(LocalReqdProp, MExpr, WinCost, Last);

            // update the upperbound of the current context
             ((Context) ssp.vc.get(ContextID)).setUpperBound(CostSoFar);

            //PTRACE0("New winner, update upperBd : " + CostSoFar.Dump());

            if (Last)
                // this's the last task for the group, so mark the group with completed optimizing
                MExpr.getGroup().set_optimized(true);

            return;
        }
    } // perform

    void TerminateThisTask(Winner LocalWinner) {
        // PTRACE("O_INPUTS %s", "this task is terminating.");
        //delete (void*) CostSoFar;

        // if this is the last task in the group, set the localwinner done=true
        if (Last) {
            LocalWinner.setDone();

            //MExpr * TempME = LocalWinner.GetMPlan();
            //os.Format(
            //    "Terminate: replaced winner with %s, %s, %s\r\n",
            //    LocalReqdProp - > Dump(),
            //    TempME ? TempME.Dump() : " null ",
            //    LocalWinner.GetCost().Dump());

        }

        if (Last)
            // this's the last task for the group, so mark the group with completed optimizing
            ssp.GetGroup(MExpr.getGrpID()).set_optimized(true);

        if (ssp.NO_PHYS_IN_GROUP)
            // delete this physical mexpr to save memory; it may not be used again.
            MExpr = null;

        // tasks must destroy themselves
        // XXX vpapad: Let's hope that the garbage collector does...
    }
    
    public String toString() {
        return "Optimizing inputs of " + MExpr;
    }
} 