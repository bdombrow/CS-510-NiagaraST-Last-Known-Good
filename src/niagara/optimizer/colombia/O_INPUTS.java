/* $Id: O_INPUTS.java,v 1.7 2003/09/13 03:33:19 vpapad Exp $
   Colombia -- Java version of the Columbia Database Optimization Framework

   Copyright (c)    Dept. of Computer Science , Portland State
   University and Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS, THE DEPT. OF COMPUTER SCIENCE DEPT. OF PORTLAND STATE
   UNIVERSITY AND DEPT. OF COMPUTER SCIENCE & ENGINEERING AT OHSU ALLOW
   USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, AND THEY DISCLAIM ANY
   LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE
   USE OF THIS SOFTWARE.

   This software was developed with support of NSF grants IRI-9118360,
   IRI-9119446, IRI-9509955, IRI-9610013, IRI-9619977, IIS 0086002,
   and DARPA (ARPA order #8230, CECOM contract DAAB07-91-C-Q518).
*/

package niagara.optimizer.colombia;

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

    private MExpr mexpr; // expression whose inputs we are optimizing
    private int arity;
    private int inputNo; // input currently being or about to be optimized, initially 0
    private int prevInputNo; // keep track of the previous optimized input no
    private Cost localCost; // the local cost of the mexpr
    private boolean last; // if this task is the last task for the group
    private Cost epsBound;
    // if global eps pruning is on, this is the eps bound for eps pruning
    // else it is zero

    //Costs and properties of input winners and groups.  Computed incrementally
    // by this method.
    Cost[] inputCost;
    LogicalProperty[] inputLogProp;

    public O_INPUTS(MExpr MExpr, Context context, boolean last) {
        this(MExpr, context, last, null);
    }

    public O_INPUTS(
        MExpr MExpr,
        Context context,
        boolean last,
        Cost bound) {
        super(MExpr.getGroup().getSSP(), context);
        this.mexpr = MExpr;
        inputNo = -1;
        this.last = last;
        prevInputNo = -1;
        epsBound = bound;

        assert(MExpr.getOp().is_physical() || MExpr.getOp().is_item());
        //We can only calculate cost for physical operators

        //Cache local properties
        Op Op = (PhysicalOp) (MExpr.getOp()); // the op of the expr
        arity = Op.getArity(); // cache arity of mexpr

        // create the arrays of input costs and logical properties
        if (arity > 0) {
            inputCost = new Cost[arity];
            inputLogProp = new LogicalProperty[arity];
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
        //Cache local properties of G and the expression being optimized

        assert mexpr.getOp().is_physical() 
            : "We can only optimize the inputs of physical expressions";
        PhysicalOp Op = (PhysicalOp) mexpr.getOp(); //the op of the expr

        Group LocalGroup = mexpr.getGroup();

        PhysicalProperty LocalReqdProp = context.getPhysProp();
        //What prop is required
        Cost LocalUB = context.getUpperBd();

        //if global eps pruning happened, terminate this task
        if (ssp.GlobepsPruning && context.isFinished()) {
            //PTRACE("%s", "Task terminated due to global eps pruning");
            if (last)
                // this's the last task for the group, so mark the group with completed optimizing
                LocalGroup.setOptimized(true);
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
        if (inputNo == -1) {
            // init inputLogProp
            for (input = 0; input < arity; input++) {
                Group InputGroup = mexpr.getInput(input);
                //Group of current input
                inputLogProp[input] = InputGroup.getLogProp();
            }

            // get the localcost of the mexpr being optimized in G
            localCost =
                Op.findLocalCost(
                    ssp.getCatalog(),
                    inputLogProp);

            //For each input group IG
            for (input = 0; input < arity; input++) {
                // Initial values of InputCost are zero in the Starburst (no Pruning) case
                if (!ssp.Pruning) {
                    if (input == 0)
                        //                        PTRACE(
                        //                            "%s",
                        //                            "Not pruning so all InputCost elements are set to zero");
                        assert(!ssp.CuCardPruning);
                    inputCost[input] = Zero;
                    continue;
                }

                IG = mexpr.getInput(input); //Group of current input

                PhysicalProperty[] properties;
                PhysicalProperty ReqProp = null;
                if (Op.is_physical()) {
                    // Determine property required of that input
                    properties =
                        ((PhysicalOp) Op).inputReqdProp(
                            LocalReqdProp,
                            inputLogProp[input],
                            input);
                    // if not possible, means no such input prop can satisfied
                    if (properties == null) {
                        //PTRACE("Impossible search: Bad input %d", input);
                        terminateThisTask(LocalWinner);
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
                    terminateThisTask(LocalWinner);
                    return;
                }
                //If search_circle returns a non-null Winner from InputGroup, case (2)
                //InputCost[InputGroup] = cost of that winner
                else if (sr.haveWinner()) {
                    inputCost[input] = IG.getWinner(ReqProp).getCost();
                    assert IG.getWinner(ReqProp).getDone() : "XXX @$@#$@#$";
                }
                //else if (!CuCardPruning) //Group Pruning case (since Starburst not relevant here)
                //InputCost[IG] = 0
                else if (!ssp.CuCardPruning)
                    inputCost[input] = Zero;
                //remainder applies only in CuCardPruning case
                else
                    inputCost[input] = IG.getLowerBd();

                IGContext = null;
            } // initialize some O_INPUTS members

            inputNo++;
            // Ensure that previous code will not be executed again; begin with Input 0
        }

        //If Global Pruning and cost so far is greater than upper bound for this context, then terminate
        CostSoFar.finalCost(localCost, inputCost);
        if (ssp.Pruning && CostSoFar.greaterThanEqual(LocalUB)) {
            //            PTRACE2(
            //                "Expr LowerBd %s, exceed Cond UpperBd %s,Pruning applied!",
            //                (const char *) CostSoFar.Dump(),
            //                (const char *) LocalUB.Dump());

            terminateThisTask(LocalWinner);
            return;
        }

        //Calculate the cost of remaining inputs
        for (input = inputNo; input < arity; input++) {
            //set up local variables
            IG = mexpr.getInput(input); //Group of current input
            //generate appropriate property for search of IG
            PhysicalProperty[] properties = null;
            PhysicalProperty ReqProp = null;
            if (Op.is_physical()) {
                // Determine property required of that input
                properties =
                    ((PhysicalOp) Op).inputReqdProp(
                        LocalReqdProp,
                        inputLogProp[input],
                        input);

                if (ssp.Pruning)
                    assert(properties != null) : " XXX @#$@#$@#$#";
                // should be possible since in the first pass, we checked it

                if (properties == null) {
                    ReqProp = null;
                    terminateThisTask(LocalWinner);
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
                terminateThisTask(LocalWinner);
                return;
            }

            //else if Case 2: There is a winner with nonzero plan, in current input
            else if (sr.haveWinner()) {
                //PTRACE("Found Winner for Input : %d", input);
                Winner Winner = IG.getWinner(ReqProp);
                assert Winner.getDone() : "XXX ^$$%^$%^$%^$%^%";

                //store its cost in InputCost[]
                inputCost[input] = Winner.getCost();

                CostSoFar.finalCost(localCost, inputCost);
                //if (Pruning && CostSoFar >= upper bound) terminate this task
                if (ssp.Pruning && CostSoFar.greaterThanEqual(LocalUB)) {
                    //                    PTRACE2(
                    //                        "Expr LowerBd %s, exceed Cond UpperBd %s,Pruning applied!",
                    //                        (const char *) CostSoFar.Dump(),
                    //                        (const char *) LocalUB.Dump());
                    //                    PTRACE("This happened at group %d ", IGNo);

                    IGContext = null;
                    terminateThisTask(LocalWinner);
                    return;
                }
                IGContext = null;
            }

            //Remaining cases are (3) and (4)
            else if (input != prevInputNo) {
                // no winner, and we did not just return from O_GROUP
                //                PTRACE("No Winner for Input : %d", input);

                //Adjust PrevInputNo and InputNo to track progress after returning from pushes
                prevInputNo = input;
                inputNo = input;

                //push this task
                ssp.addTask(this);

                //Build a context for the input group task
                //First calculate the upper bound for search of input group.
                //Upper bounds are irrelevant unless we are pruning
                Cost inputBound = new Cost(LocalUB);
                //Start with upper bound of G's context
                if (ssp.Pruning) {
                    //                    PTRACE0("LocalCost is " + LocalCost.Dump());
                    CostSoFar.finalCost(localCost, inputCost);
                    inputBound.subtract(CostSoFar); //Subtract CostSoFar
                    inputBound.add(inputCost[input]);
                    //Add IG's contribution to CostSoFar
                }

                PhysicalProperty InputProp = new PhysicalProperty(ReqProp);
                // update the bound in multiwinner to InputBd
                Context inputContext = new Context(InputProp, inputBound, false);
                if (ssp.GlobepsPruning) {
                    Cost eps_bound;
                    if (epsBound.greaterThan(localCost)) {
                        eps_bound = new Cost(epsBound);
                        // calculate the cost, the lower nodes should have lower eps bound
                        eps_bound.subtract(localCost);
                    } else
                        eps_bound = new Cost(0);
                    if (arity > 0)
                        eps_bound.divide(arity);
                    ssp.addTask(
                        new O_GROUP(
                            ssp,
                            mexpr.getInput(input),
                            inputContext,
                            true,
                            eps_bound));
                } else
                    ssp.addTask(
                        new O_GROUP(ssp, mexpr.getInput(input), inputContext));

                return;
            } else { // We just returned from O_GROUP on IG
                // impossible plan for this context
                //                PTRACE(
                //                    "impossible plan since no winner possible at input %d",
                //                    InputNo);

                terminateThisTask(LocalWinner);
                return;
            }
        } //Calculate the cost of remaining inputs

        // If arity is zero, we need to ensure that this expression can
        // satisfy this required property.
        if (arity == 0
            && !LocalReqdProp.getOrder().isAny()
            && Op.is_physical()) {
            PhysicalProperty OutputPhysProp = ((PhysicalOp) Op).findPhysProp(PhysicalOp.NO_INPUTS);
            if (!(LocalReqdProp == OutputPhysProp)) {
                //                PTRACE2(
                //                    "physical epxr: %s does not satisfy required phys_prop: %s",
                //                    (const char *) MExpr.Dump(),
                //                    (const char *) LocalReqdProp.Dump());
                OutputPhysProp = null;

                terminateThisTask(LocalWinner);
                return;
            }
        }

        //All inputs have been been optimized, so compute cost of the expression being optimized.

        if (ssp.FIRSTPLAN) {
            //If we are in the root group and no plan in it has been costed
            if (!(mexpr.getGrpID() == 0) && !(LocalGroup.getfirstplan())) {
                // OUTPUT("First Plan is costed at task %d\r\n", TaskNo);
                LocalGroup.setfirstplan(true);
            }
        }
        CostSoFar.finalCost(localCost, inputCost);
        // PTRACE0("Expression's Cost is " + CostSoFar.Dump());
        // OUTPUT("COSTED %s  ", MExpr - > Dump());
        // OUTPUT("%s\r\n", CostSoFar.Dump());
        if (ssp.GlobepsPruning) {
            //PTRACE0("Current Epsilon Bound is " + EpsBound.Dump());
            //If global epsilon pruning is on, we may have an easy winner
            if (epsBound.greaterThanEqual(CostSoFar)) {
                //we are done with this search, we have a final winner
                //                PTRACE(
                //                    "Global Epsilon Pruning fired, %s",
                //                    "got a final winner for this context");

                Cost WinCost = new Cost(CostSoFar);
                //Cost WinCost(CostSoFar);
                LocalGroup.newWinner(LocalReqdProp, mexpr, WinCost, true);
                // update the upperbound of the current context
                context.setUpperBound(CostSoFar);
                context.setFinished();
                terminateThisTask(LocalWinner);
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

            terminateThisTask(LocalWinner);
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
            terminateThisTask(LocalWinner);
            return;
        }
        else {
            //The expression being optimized is a new winner
            Cost WinCost = new Cost(CostSoFar);
            LocalGroup.newWinner(LocalReqdProp, mexpr, WinCost, last);

            // update the upperbound of the current context
             context.setUpperBound(CostSoFar);

            //PTRACE0("New winner, update upperBd : " + CostSoFar.Dump());

            if (last)
                // this's the last task for the group, so mark the group with completed optimizing
                mexpr.getGroup().setOptimized(true);

            return;
        }
    } // perform

    void terminateThisTask(Winner LocalWinner) {
        // PTRACE("O_INPUTS %s", "this task is terminating.");
        //delete (void*) CostSoFar;

        // if this is the last task in the group, set the localwinner done=true
        if (last) {
            LocalWinner.setDone();

            //MExpr * TempME = LocalWinner.GetMPlan();
            //os.Format(
            //    "Terminate: replaced winner with %s, %s, %s\r\n",
            //    LocalReqdProp - > Dump(),
            //    TempME ? TempME.Dump() : " null ",
            //    LocalWinner.GetCost().Dump());

        }

        if (last)
            // this's the last task for the group, so mark the group with completed optimizing
            mexpr.getGroup().setOptimized(true);

        if (ssp.NO_PHYS_IN_GROUP)
            // delete this physical mexpr to save memory; it may not be used again.
            mexpr = null;

        // tasks must destroy themselves
        // XXX vpapad: Let's hope that the garbage collector does...
    }
    
    public String toString() {
        return "Optimizing inputs of " + mexpr;
    }
} 
