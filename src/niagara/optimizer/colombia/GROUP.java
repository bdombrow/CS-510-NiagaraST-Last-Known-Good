/* $Id: GROUP.java,v 1.10 2003/09/16 04:45:29 vpapad Exp $ 
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

import java.util.ArrayList;

/*
  The main problem, and its subproblems, consist of a search to find
  the cheapest MultiExpression in a Group, satisfying some context.
  
  A Group includes a collection of logically equivalent MExpr's.  The
  Group also contains logical properties shared by all MExpr's in the
  group.
	 
  A Group also contains a winner's circle consisting of winners from
  previous searches of the Group.  Note that each search may of the
  Group may have different contexts and thus different winners.
	   
  A Group can also be thought of as a temporary collection, or
  subquery, in a computation of the main query.
	 
  We assume the following three conditions are equivalent:
  (1) There is a winner (maybe with null *plan) for some property, 
  with done==true

  (2) The group contains all possible logical and physical expressions
  except for enforcers 
  
  (3) The bit "optimized" is turned on.

  And any of these imply:
  (4) For any property P, the cheapest plan for property P is either
  in G or is an enforcer for P

  The truth of the above assumptions depend on whether we fire all
  applicable rules when the property is ANY. 
*/

public class Group {

    private SSP ssp;

    // XXX vpapad: from defs.h - is used in other files too...
    // cucard pruning flag
    private static final boolean CUCARD_PRUNING = false;

    // XXX vpapad: replaced bits with booleans for now
    private boolean changed; // has the group got changed or just created?
    // Used for tracing
    private boolean exploring; // is the group being explored? 
    private boolean explored; // Has the group been explored?
    private boolean optimizing; //is the group being optimized?
    private boolean optimized; // has the group been optimized (completed) ?

    // XXX vpapad public:

    //Create a new Group containing just this MExpression
    public Group(MExpr mexpr, SSP ssp) {
        ssp.getTracer().beforeNewGroup(mexpr);
        groupID = mexpr.getGrpID();
        firstLogMExpr = lastLogMExpr = mexpr;
        this.ssp = ssp;

        assert mexpr
            .getOp()
            .isLogical() : "Group initialized by physical multiexpression";

        LogicalOp op = (LogicalOp) mexpr.getOp();

        // find the log prop
        int arity = mexpr.getArity();
        LogicalProperty[] inputProp = new LogicalProperty[arity];
        if (arity == 0) {
            logProp = op.findLogProp(ssp.getCatalog(), inputProp);
        } else {
            for (int i = 0; i < arity; i++) {
                inputProp[i] = mexpr.getInput(i).getLogProp();
            }

            logProp = op.findLogProp(ssp.getCatalog(), inputProp);
        }

        /* Calculate the LowerBd, which is:
           TouchCopyCost:        
           touchcopy() * |G| +     //From top join
           // from other, nontop, joins
           touchcopy() * sum(cucard(Ai) i = 2, ..., n-1) +  
           + FetchCost:
           fetchbound() * sum(cucard(Ai) i = 1, ..., n) // from leaf fetches
        */
        double cost = 0;
        if (mexpr.getOp().isLogical()) {
            // XXX vpapad: what is this doing here? 
            if (mexpr.getOp().getName().equals("GET"))
                cost = 0; // GET operator does not have a CopyOut cost
            else
                cost = touchCopyCost(logProp);

            // Add in fetching cost if CuCard Pruning 
            if (CUCARD_PRUNING)
                cost += fetchingCost(logProp);
        }

        lowerBd = new Cost(cost);

        mexpr.setGroup(this);

        // the initial value is -1, meaning no winner has been found
        count = -1;

        winners = new ArrayList();
        ssp.getTracer().afterNewGroup(this);
    }

    //Find first and last (in some sense) MExpression in this Group
    public MExpr getFirstLogMExpr() {
        return firstLogMExpr;
    }
    
    public MExpr getLastLogMExpr() {
        return lastLogMExpr;
    }

    MExpr getFirstPhysMExpr() {
        return firstPhysMExpr;
    }
    void setLastLogMExpr(MExpr last) {
        lastLogMExpr = last;
    }
    void setLastPhysMExpr(MExpr last) {
        lastPhysMExpr = last;
    }

    //clear winner, used for multi-phased optimization
    public void clear_winners() {
        winners.clear();
    }

    public boolean isExplored() {
        return explored;
    }
    public void setExplored(boolean explored) {
        this.explored = explored;
    }

    public boolean isChanged() {
        return changed;
    }
    void setChanged(boolean changed) {
        this.changed = changed;
    }

    public boolean isOptimized() {
        return optimized;
    }
    public void setOptimized(boolean is_optimized) {
        optimized = is_optimized;
    }

    boolean isExploring() {
        return exploring;
    }
    void setExploring(boolean is_exploring) {
        exploring = is_exploring;
    }

    public LogicalProperty getLogProp() {
        return logProp;
    }

    void setLogProp(LogicalProperty t) {
        logProp = t;
    }

    Cost getLowerBd() {
        return lowerBd;
    }

    int getCount() {
        return count;
    }

    int getGroupID() {
        return groupID;
    }

    // XXX vpapad #ifdef FIRSTPLAN
    void setfirstplan(boolean firstplan) {
        Group.firstplan = firstplan;
    }
    boolean getfirstplan() {
        return firstplan;
    }

    private int groupID; //ID of this group

    private MExpr firstLogMExpr; //first log MExpr in  the Group
    private MExpr lastLogMExpr; //last log MExpr in  the Group
    private MExpr firstPhysMExpr; //first phys MExpr in  the Group
    private MExpr lastPhysMExpr; //last phys MExpr in  the Group

    private LogicalProperty logProp; //Logical properties of this Group
    private Cost lowerBd;
    // lower bound of cost of fetching cucard tuples from disc

    // Winner's circle
    private ArrayList winners;

    private int count;

    // XXX vpapad #ifdef FIRSTPLAN
    private static boolean firstplan = false;

    //Temporary - for debugging only
    Winner getWinnerByID(int id) {
        if (winners.size() <= id)
            return null;
        else
            return (Winner) winners.get(id);
    }

    /* boolean search_circle(Context * C, boolean & moresearch) 
    {
    First search for a winner with property P.
    If there is no such winner, case (3)
    If there is a winner, denote its plan component by WPlan and 
    its cost component by WCost.
    Context cost component is CCost
    
      If (WPlan is non-null) //Cheapest plan costs *WCost;
      //we seek a plan costing *CCost or less
      If (*WCost <= *CCost) 
        Case (2)
      else if (*CCost < *WCost)
             Case (1)
           else If (WPlan is null) //All plans cost more than *WCost
             if(*CCost <= *WCost)
               Case (1)
             else if (*WCost < *CCost) 
                   //There might be a plan between WCost and CCost
               Case (4)
    */

    /*search_circle returns the state of the winner's circle for this
      context and group - it does no rule firing.  Thus it is cheap to execute.
      search_circle returns in four possible states:
      (1) There is no possibility of satisfying C
      (2) There is a non-null winner for C
      (3) More search is needed, and there has been no search 
      for this property before
      (4) More search is needed, and there has been a search for
      this property before.
      See the pseudocode for search_circle (in *.cpp) for how these states arise.
      
      search_circle could adjust the current context, but won't because:
      It is not necessary for correctness
      It is likely not very useful
      
    */
    public static class SearchResults {
        private SearchResults() {
        }
        public static final SearchResults IMPOSSIBLE = new SearchResults();
        public static final SearchResults HAVE_WINNER = new SearchResults();
        public static final SearchResults STARTING_SEARCH = new SearchResults();
        public static final SearchResults CONTINUING_SEARCH =
            new SearchResults();
        public final boolean isImpossible() {
            return this == IMPOSSIBLE;
        }
        public final boolean haveWinner() {
            return this == HAVE_WINNER;
        }
        public final boolean isStartingSearch() {
            return this == STARTING_SEARCH;
        }
        public final boolean isContinuingSearch() {
            return this == CONTINUING_SEARCH;
        }
    }

    public SearchResults search_circle(Context c) {
        //First search for a winner with property P.
        Winner winner = getWinner(c.getPhysProp());

        //If there is no such winner, case (3)
        if (winner == null)
            return SearchResults.STARTING_SEARCH;

        assert winner.getDone() : "Winner should be done";
        //This is not a recursive query

        //If there is a winner, denote its plan, cost components by M
        //and WCost
        //Context cost component is CCost
        MExpr m = winner.getMPlan();
        Cost wCost = winner.getCost();
        Cost cCost = c.getUpperBd();
        assert cCost.nonZero(); //Did we get rid of all cruft?

        if (m != null) {
            if (cCost.greaterThanEqual(wCost)) // there is a non-null winner
                return SearchResults.HAVE_WINNER;
            else // search is impossible as winner's cost is more than
                // required context cost
                return SearchResults.IMPOSSIBLE;
        } else {
            if (wCost.greaterThanEqual(cCost))
                return SearchResults.IMPOSSIBLE;
            else
                return SearchResults.CONTINUING_SEARCH;
        }
    }

    //Return winner for this property, null if there is none
    Winner getWinner(PhysicalProperty physProp) {
        int size = winners.size();
        for (int i = 0; i < size; i++) {
            // XXX vpapad have to redefine PhysProp equals
            if (physProp.equals(((Winner) winners.get(i)).getPhysProp()))
                return (Winner) winners.get(i);
        }

        //No matching winner
        return null;
    }

    //Make this the new winner for this group
    //If doneonely is true, then find the winnner for the given property and
    //change its done parameter to true
    void newWinner(
        PhysicalProperty reqdProp,
        MExpr mexpr,
        Cost totalCost,
        boolean done) {
        changed = true;

        //construct a new winner
        Winner win = new Winner(mexpr, reqdProp, totalCost, done);

        ssp.getTracer().newWinner(this, win);

        //Find it in the winner's circle
        //Look for a winner with that property in the winner's circle
        for (int i = winners.size(); --i >= 0;) {
            PhysicalProperty winPhysProp = ((Winner) winners.get(i)).getPhysProp();

            if (winPhysProp.equals(reqdProp)) {
                winners.set(i, win);
                //the count is set to 0 when a not null new winner is found
                if (mexpr != null && totalCost.nonZero())
                    count = 0;
                return;
            }
        }

        //No matching winner for this property, so add new winner
        winners.add(win);

        // set count for this property
        count = -1;
        ssp.getTracer().newWinner(this, win);
    }

    //check if there is at least one winner done in this group
    boolean checkWinnerDone() {
        //Search Winner's circle.  If there is a winner done, return true
        int size = winners.size();
        for (int i = 0; i < size; i++) {
            if (((Winner) winners.get(i)).getDone())
                return true;
        }

        //No winner is done
        return false;
    }

    //    // Group::CatalogUpdate()
    //    //	- Compute the log prop for this group, based on a catalog
    //    //	- Update GET::Coll, FILE_SCAN::Coll, according to current catalog
    //    // Used by SSP::Plans_And_Costs() to compute 
    //    // the cost distribution under various catalog
    //    public LogicalProperty catalogUpdate() {
    //	if (logProp != null) 
    //	    return logProp;
    //	
    //	// Update GET::Coll
    //	for(MExpr m = getFirstLogMExpr(); m != null; m = m.getNextMExpr()){
    //	    if(m.getOp().getName().equals("GET")) {
    //		GET get = (GET) m.getOp();
    //		get.setColl(CAT.Cat.getColl(get.getColl().getName()));
    //	    }
    //	}
    //
    //	// Update FILE_SCAN::Coll
    //	for(MExpr m = getFirstPhysMExpr(); m != null; m = m.getNextMExpr()){
    //	    if(m.getOp().getName().equals("FILE_SCAN")) {
    //		FILE_SCAN filescan = (FILE_SCAN) m.getOp();
    //		filescan.setColl(
    //		    CAT.Cat.getColl(filescan.getColl().getName()));
    //	    }
    //	}

    //	// get the first logical mexpression 
    //	MExpr mexpr = getFirstLogMExpr();
    //	ArrayList iprops = new ArrayList();
    //	for(int i = 0; i < mexpr.getArity(); i++) {
    //	    Ssp.getGroup(mexpr.getInput(i)).catalogUpdate();
    //	    iprops.add(Ssp.getGroup(mexpr.getInput(i)).getLogProp());
    //	}
    //	logProp = mexpr.getOp().findLogProp(iprops);
    //	return logProp;
    //    }

    // // free up memory
    // ~Group() {
    // 	if (!ForGlobalEpsPruning) ClassStat[C_GROUP].Delete();

    // 	delete LogProp;
    // 	delete LowerBd;

    // 	MExpr * mexpr =firstLogMExpr;
    // 	MExpr * next = mexpr;
    // 	while(next!=null)
    // 	{	next = mexpr.GetNextMExpr();
    // 		delete mexpr; 
    // 		mexpr = next;
    // 	}

    // 	mexpr =firstPhysMExpr;
    // 	next = mexpr;
    // 	while(next!=null)
    // 	{	next = mexpr.GetNextMExpr();
    // 		delete mexpr; 
    // 		mexpr = next;
    // 	}

    // 	for(int i = 0; i < Winners.size(); i++)
    // 		delete Winners[i];

    // }

    //Add a new MExpr to the group
    void newMExpr(MExpr mexpr) {

        mexpr.setGroup(this);

        // link to last mexpr
        if (mexpr.getOp().isLogical()) {
            // No need to check that (first/last)LogMEXpr != null 
            // -- groups are created with at least one logical expression 
            lastLogMExpr.setNextMExpr(mexpr);
            lastLogMExpr = mexpr;
        } else {
            if (lastPhysMExpr != null)
                lastPhysMExpr.setNextMExpr(mexpr);
            else
                firstPhysMExpr = mexpr;
            lastPhysMExpr = mexpr;
        }

        // if there is a winner found before, count the number of plans
        if (count != -1)
            count++;

        // XXX vpapad: adding mexpr to group -> do tracing
    }

    // XXX vpapad this seems to be unused
    //    void shrinkSubGroup() {
    //	for(MExpr mexpr = firstLogMExpr; mexpr != null; 
    //	    mexpr = mexpr.getNextMExpr()) {
    //	    for(int i = 0; i < mexpr.getArity(); i++)
    //		// XXX vpapad: make MExpr::getInput return Group
    //		// so that we won't have to go through SSP for this?
    //		Ssp.shrinkGroup(mexpr.getInput(i));
    //	}
    //    }

    // Delete a physical MExpr from a group, save memory
    public void deletePhysMExpr(MExpr physMExpr) {
        MExpr mexpr = firstPhysMExpr;
        MExpr next;
        if (mexpr == physMExpr) {
            firstPhysMExpr = mexpr.getNextMExpr();
            // if the MExpr to be deleted is the only one in the group, 
            // set both First and Last Physical MExpr to be null
            if (firstPhysMExpr == null)
                lastPhysMExpr = null;
        } else {
            // search for the MExpr to be deleted in the link list
            while (mexpr != null) {
                next = mexpr.getNextMExpr();
                // if found, manipulate the pointers 
                // and delete the necessary one
                if (next == physMExpr) {
                    mexpr.setNextMExpr(next.getNextMExpr());
                    if (next.getNextMExpr() == null)
                        lastPhysMExpr = mexpr;
                    break;
                }
                mexpr = mexpr.getNextMExpr();
            }
        }
    }

    /* need for group pruning, calculate the TouchCopy cost of the expr
       TouchCopyCost = 
            TouchCopy() * |G| +     //From top join
            TouchCopy() * sum(cucard(Ai) i = 2, ..., n-1) +  // from other, nontop,
    */ // joins
    // TouchCopyCost are not supported as my model does not include base collection 
    // information for intermediate, which, however, can be accomplished.
    // Now only compute the copy cost for the intermediate result.
    // Quan
    // 12/99 
    double touchCopyCost(LogicalProperty logProp) {
        double total;

        if (logProp.getCardinality() == -1)
            total = 0; // Card is unknown
        else
            total = logProp.getCardinality(); // from top join

        /*  // from A2 -- An-1 , means excluding the min and max cucard(i)
            double Min= 3.4E+38; 
            double Max=0;
            for(int i=0; i < LogProp->Schema.GetTableNum(); i++)   
            {   
                //float CuCard = LogProp->Schema.GetTableMaxCuCard(i);
                float CuCard = LogProp.(*Attrs)[i]->CuCard;
                if(Min > CuCard) Min = CuCard;
                if(Max < CuCard) Max = CuCard;
                Total += CuCard ;
            }
        
            // exclude min and max
            Total -= Min;
            Total -= Max;
        */
        // XXX vpapad not handling cost model yet
        //   return total * Cm.touch_copy();
        return 1;
    }

    // String Dump() {
    //     String os;
    //     int Size = 0;
    //     String temp;
    //     MExpr* MExpr;

    //     os.Format("\r\n\r\n%s%d%s\r\n","Group ", GroupID , " : -------------");

    //     for(MExpr=firstLogMExpr; MExpr!=null; MExpr = MExpr.GetNextMExpr()) {
    // 	os += MExpr.Dump();
    // 	os += " ; ";
    // 	Size++;
    //     }
    //     for(MExpr=firstPhysMExpr; MExpr!=null; MExpr = MExpr.GetNextMExpr()) {
    // 	os += MExpr.Dump();
    // 	os += " ; ";
    // 	Size++;
    //     }
    //     temp.Format("\t %s%d%s", " [Totally " , Size , " MExprs]\r\n");
    //     os += temp;

    //     //Print Winner's circle
    //     os += "Winners:" ;

    //     Size = Winners.size();
    //     PhysicalProperty * PhysProp;
    //     if(!Size) os += "\tNo Winners\r\n";
    //     for(int i = 0; i < Size; i++) {
    // 	PhysProp = Winners[i] -> GetPhysProp();
    // 	os += "\t" ;
    // 	os += PhysProp -> Dump();
    // 	os += ", " ;
    // 	os += (Winners[i].GetMPlan() ? Winners[i].GetMPlan()-> Dump() : "null Plan");
    // 	os += ", " ;
    // 	os += (Winners[i].GetCost() ? Winners[i].GetCost().Dump() : "null Cost");
    // 	os += ", " ;
    // 	os += (Winners[i].GetDone() ? "Done" : "Not done");
    // 	os += "\r\n";
    //     }
    // #endif

    //     os += "LowerBound: " + LowerBd.Dump() + "\n";

    //     os += "log_prop: ";
    //     os += (*LogProp).Dump();

    //     return os;
    // }

    /* for cucard pruning, calculate the minimun cost of fetching cucard tuples from disc
        FetchingCost = 
            Fetch() * sum(cucard(Ai) i = 1, ..., n) // from leaf fetches
        For each Ai which has no index on a join (interesting) order, replace
        cucard(Ai) with |Ai| and it is still a lower bound.
    */

    // FetchingCost are not currently supported as my model does not include base collection 
    // information for intermediate, which, however, can be accomplished.
    // Now, return only the cost of fetching visible attribtes at the current point.
    // Quan 
    // 10/99

    double fetchingCost(LogicalProperty logProp) {
        double Total = 0;

        /*  for(int i=0; i < LogProp-> Schema.GetTableNum(); i++)
            {   
                float CuCard = LogProp->Schema.GetTableMaxCuCard(i);
                float Width = LogProp->Schema.GetTableWidth(i);
                Total += ceil(CuCard * Width) * 
                         (             // cpu cost of reading from disk
                            Cm.io());                 // i/o cost of reading from disk
            }
        */
        for (int i = 0; i < logProp.getAttrs().size(); i++) {
            // XXX vpapad: we don't support attr props any more
            //            float CuCard =
            //                logProp.GetAttrProp(logProp.GetAttr(i).GetName()).getCuCard();
            // float Width = logProp.GetAttr(i).GetWidth();
            // XXX vpapad not handling cost model yet
            //        Total += ceil(CuCard * Width) 
            //                 * Cm.io()             // i/o cost of reading from disk
            //                 / Cm->page_size();
        }
        //    return Total;
        return 1;
    }
    /**
     * Returns the ssp.
     * @return SSP
     */
    public SSP getSSP() {
        return ssp;
    }

    public String toString() {
        return "group " + getGroupID();
    }

    public boolean needsExploring() {
	return !(exploring || explored || optimized);
    }
}
