package niagara.optimizer.colombia;

/**
   SEARCH SPACE 
   ============================================================
   We borrow the term Search Space from AI, where it is a tool for solving a 
   problem  In query optimization the problem is to find the cheapest plan 
   for a given query subject to certain context (class CONT).
   
   A Search Space typically consists of a collection of possible solutions
   to the problem and its subproblems. Dynamic Programming and
   Memoization are two approaches to using a Search Space to solve a problem.
   Both Dynamic Programming and Memoization partition the possible solutions 
   by logical equivalence. We will call each partition a GROUP.  Thus a GROUP
   contains a collection of logically equivalent expressions.
  
   In our setting of query optimization, logical equivalence is query
   equivalence.  Each GROUP corresponds to a temporary collection, or a
   subquery, in a computation of the main query.  If the main query is A
   join B join C, then there are GROUPs representing A join B, A join C,
   C, etc.
	
   In our approach to query optimization, each possible solution in the Search
   Space is represented compactly, by what we call a Multi-expression 
   (class M_EXPR).  A solution in the search space can also be represented in
   more detail by an expression (class EXPR).
*/
public class SSP {
    private int newGrpID;
    private int rulesetSize;

    public SSP(int rulesetSize) {
	newGrpID = -1;
	this.rulesetSize = rulesetSize;

// 	//initialize HashTbl to contain HashTableSize elements, each initially null.
// 	HashTbl = new M_EXPR* [HtblSize];
// 	for(ub4 i = 0 ; i < HtblSize ; i++)
// 		HashTbl[i] = null;
    }

    public void init() {
//     EXPR *	Expr = Query->GetEXPR(); 
    
//     // create the initial search space
//     RootGID = NEW_GRPID;
//     M_EXPR * MExpr = CopyIn(Expr, RootGID);
    
//     InitGroupNum = NewGrpID;
//     if(COVETrace)	//End Initializing Search Space
// 	{
//             UI::printOutputCOVE("EndInit\n");
// 	}
    }

    // return the next available grpID in SSP
    protected int getNewGrpID() { 
	return (++newGrpID); 
    }		

    // free up memory
 public void clear() {
//      for(uint i=0; i< Groups.size();i++)
//  	delete Groups[i] ;
//      for (uint j=0; j< M_WINNER::mc.size(); j++)
//  	delete M_WINNER::mc[j];
//      delete [] HashTbl;
 }

//     private static final int NEW_GRPID = -1;
//     // use by CopyIn and M_EXPR::M_EXPR
//     // means need to create a new group

//     // XXX vpapad public:
// public M_EXPR ** HashTbl;	// To identify duplicate MExprs
    
//     void Init();  //Create some default number of empty Groups, the number
//     //depending on the initial query.  Read in initial query.
	
//     void optimize();	//Later add a conditon.
//     // Prepare the SSP so an optimal plan can be found
    
//     // Convert the EXPR into a Mexpr. 
//     // If Mexpr is not already in the search space, then copy Mexpr into the 
//     // search space and return the new Mexpr.  
//     // If Mexpr is already in the search space, then either throw it away or
//     // merge groups.
//     // GrpID is the ID of the group where Mexpr will be put.  If GrpID is 
//     // NEW_GRPID(-1), make a new group with that ID.
//     M_EXPR * CopyIn(EXPR * Expr, int & GrpID);	
    
//     // Copy out the final plan.  Recursive, each time increasing tabs by
//     // one, so the plan is indented.
//     void CopyOut(int GrpID, PHYS_PROP * PhysProp, int tabs);

//     // Copy out all the logical expressions with no pruning.
//     // By default, start with the top group (GrpID=0)
//     std::vector < EXPR *> * GetExprs(int GrpID=0);
    
//     // Copy out all the physical plans with no pruning. 
//     // By default, start with the top group (GrpID=0)
//     std::vector < EXPR *> * GetPlans(int GrpID=0);
    
//     // A helper function for GetAllExprs and GetAllPlans
//     // Return all the logical expressions or physical plans with no pruning.
//     //	Logical expressions if forLogical == true
//     //	Physical plans if forLogical == false
//     std::vector<EXPR*> * AllExprs(int GrpID, PHYS_PROP * PhysProp, bool forLogical);
    
    
//     std::vector< GROUP* > * GetGroups() { return &Groups; };
//     // return the specific group
//     inline GROUP *	GetGroup(int Gid) { return Groups[Gid]; } ;	
    
//     //If another expression in the search space is identical to MExpr, return 
//     // it, else return null. 
//     // Identical means operators and arguments, and input groups are the same.
//     M_EXPR * FindDup (M_EXPR & MExpr);
	
//     //When a duplicate is found in two groups they should be merged into
//     // the same group.  We always merge bigger group_no group to smaller one.	
//     int MergeGroups(int group_no1, int group_no2);
//     //int MergeGroups(GROUP & ToGroup, GROUP & FromGroup);
    
//     void  ShrinkGroup(int group_no);	//shrink the group marked completed
//     void  Shrink();			//shrink the ssp
    
//     bool IsChanged(); // is the ssp changed?
    
//     // Print groups, expressions, plans and costs
//     void Space_Plans_Costs();
    
//     CString Dump();
//     void FastDump();	
    
//     CString DumpChanged();
//     CString DumpHashTable();	
    
//     // Call init_state for every group
//     void AnotherRound();
//     // XXX vpapad private:
//     int	RootGID;
//     int	InitGroupNum;
    
//     //Collection of Groups, indexed by GRP_ID
//     std::vector < GROUP* > Groups;




// CString DumpHashTable()
// {
// 	CString os;
// 	CString temp;
    
// 	os = "Hash Table BEGIN:\r\n";
// 	int total=0;
// 	for(int i=0; i < HtblSize; i++)
// 	{
// 		for(M_EXPR *mexpr = HashTbl[i]; mexpr != null; mexpr=mexpr->GetNextHash(),total++)
// 			os += mexpr->Dump();
// 		if(HashTbl[i]) os += "\r\n";
// 	}
// 	temp.Format("Hash Table END, total %d mexpr\r\n",total);
// 	os += temp;
    
// 	return os;
// }

// CString DumpChanged()
// {
//     CString os;
//     GROUP* Group;
    
// 	for(uint i=0; i< Groups.size();i++)
// 		if(Groups[i]->is_changed()) 
// 		{
// 			Group = Groups[i];
// 			os += Group->Dump();
// 			Group->set_changed(false);
// 		}
		
// 		if(os!="") 	return ("Changed Search Space:\r\n" + os);
// 		else return ("Search Space not changed");
// }

// void Shrink()
// {
// 	for(uint i=InitGroupNum; i<Groups.size();i++)	
//             ShrinkGroup(i);
// }

// void ShrinkGroup(int group_no)
// {
//     GROUP* Group;
//     M_EXPR * mexpr;
//     M_EXPR * p;
//     M_EXPR * prev;
//     int DeleteCount=0;
    
// 	SET_TRACE Trace(true);
    
// 	Group = Groups[group_no];
    
// 	if(! Group->is_optimized() && ! Group->is_explored()) return ;   // may be pruned
    
// 	PTRACE("Shrinking group %d,", group_no);
    
// 	// Shrink the logical mexpr
// 	// init the rule mark of the first mexpr to 0, means all rules are allowed
// 	mexpr =  Group->GetFirstLogMExpr();
// 	mexpr->clear_rule_mask();
    
// 	// delete all the mexpr except the first initial one
// 	mexpr = mexpr->GetNextMExpr();
    
// 	while(mexpr != null)
// 	{
// 		// maintain the hash link
// 		// find my self in the appropriate hash bucket
// 		ub4 hashval = mexpr->hash();
// 		for (p = HashTbl[ hashval ], prev = null;
// 		p != mexpr;  
//     				prev = p, p = p -> GetNextHash()) ;
					
// 					assert(p==mexpr);
// 					// link prev's next hash to next 
// 					if(prev) 
// 						prev->SetNextHash(mexpr->GetNextHash());
// 					else
// 						// the mexpr is the first in the bucket
// 						HashTbl[ hashval ] = mexpr->GetNextHash();
					
// 					p = mexpr;
// 					mexpr = mexpr->GetNextMExpr();
					
// 					delete p; 
// 					DeleteCount ++;
// 	}
    
// 	mexpr =  Group->GetFirstLogMExpr();
// 	mexpr->SetNextMExpr(null);
// 	// update the lastlogmexpr = firstlogmexpr;
// 	Group->SetLastLogMExpr(mexpr);
    
// 	// Shrink the physcal mexpr
// 	mexpr =  Group->GetFirstPhysMExpr();
    
// 	while(mexpr != null)
// 	{
// 		p = mexpr;
// 		mexpr = mexpr->GetNextMExpr();
		
// 		delete p; 
// 		DeleteCount ++;
// 	}
    
// 	mexpr =  Group->GetFirstPhysMExpr();
// 	mexpr->SetNextMExpr(null);
// 	// update the lastlogmexpr = firstlogmexpr;
// 	Group->SetLastPhysMExpr(mexpr);
    
// 	Group->set_changed(true);
// 	Group->set_exploring(false);
	
// 	PTRACE("Deleted %d mexpr!\r\n", DeleteCount);
// }

// CString Dump()
// {
//     CString os;
//     GROUP* Group;
    
// 	os.Format("%s%d%s","RootGID:" , RootGID , "\r\n");
    
// 	for(uint i=0; i< Groups.size();i++)
// 	{
// 		Group = Groups[i];
// 		os += Group->Dump();
// 		Group->set_changed(false);
// 	}
	
// 	return os;
// }

// void FastDump()
// {
// 	TRACE_FILE("SSP Content: RootGID: %d\r\n" , RootGID);
    
// 	for(uint i=0; i< Groups.size();i++)
// 	{
// 		Groups[i]->FastDump() ;
// 		Groups[i]->set_changed(false);
// 	}
// }

// M_EXPR * FindDup (M_EXPR & MExpr)
// {
// 	int Arity = MExpr.GetArity();
	
// 	ub4 hashval = MExpr.hash ();
// 	M_EXPR * prev = HashTbl[hashval];
    
// 	int BucketSize = 0;
// 	if (!ForGlobalEpsPruning) OptStat->HashedMExpr ++;
// 	// try all expressions in the appropriate hash bucket
// 	for (M_EXPR * old = prev;  old != null;  prev = old, old = old -> GetNextHash(), BucketSize++)
// 	{
		
// 		int input_no;
		
// 		// See if they have the same arities
// 		if (old -> GetArity() != Arity) {
// 			goto not_a_duplicate;
// 		}
		
// 		// finding yourself does not constitute a duplicate
// 		// compare pointers to see if EXPR_LISTs are the same
// 		if (old == &MExpr) {
// 			goto not_a_duplicate;
// 		}
		
// 		// compare the inputs
// 		// Compare the actual group pointers for every input
// 		for (input_no = Arity;  -- input_no >= 0; ) 
// 			if (MExpr.GetInput(input_no) != old->GetInput(input_no))
// 			{
// 				PTRACE("Different at input %d", input_no);
// 				goto not_a_duplicate;
// 			}
			
// 			// finally compare the Op
// 			// Modified by Quan 12/99 from
// 			//		if(! (*(old->GetOp()) == MExpr.GetOp())) 
// 			//if(! (*old->GetOp() == * MExpr.GetOp())) 
// 			if(! (*(old->GetOp()) == * (MExpr.GetOp()))) 
// 			{
//                             PTRACE2("Different at Operator. %s : %s", 
//                                     (const char *) old->Dump(), 
//                                     (const char *) MExpr.Dump());
//                 goto not_a_duplicate;
//             }
			
// 			// "expr" is a duplicate of "old"
// 			return (old);
			
// not_a_duplicate :
// 			continue;       // check next expression in hash bucket
// 	} // try all expressions in the appropriate hash bucket
    
// 	// no duplicate, insert into HashTable
// 	if(prev == null) 
// 		HashTbl[hashval] = & MExpr;
// 	else 
// 		prev->SetNextHash(& MExpr);
    
// 	if (!ForGlobalEpsPruning)
// 	{
// 		if(OptStat->MaxBucket < BucketSize) OptStat->MaxBucket = BucketSize;
// 	}
    
// 	return (null);
// } // FindDup

// // merge two groups when duplicate found in these two groups
// // means they should be the same group
// // always merge bigger group_no group to smaller one.

// // Merge already existing group is problematic!! What about parents.  
// // QuanW 5/ 2000 

// int MergeGroups(int group_no1, int group_no2)
// {
//     //M_EXPR * mexpr;
    
// 	int ToGid = group_no1;
// 	int FromGid = group_no2;
    
// 	// always merge bigger group_no group to smaller one.
// 	if(group_no1 > group_no2) 
// 	{
// 		ToGid = group_no2;
// 		FromGid = group_no1;
// 	}
    
// #ifdef UNIQ
// 	//	assert(false);
// 	PTRACE("Merge group %d and %d\r\n", group_no1, group_no2); 
// 	PTRACE("!!!DUPLICTES DETECTED WHILE UNIQ IS SET!!!%s\r\n", "");
// #endif
    
// 	return ToGid;
// }// MergeGroups


// M_EXPR*	CopyIn(EXPR * Expr, int& GrpID)
// {
// 	GROUP *	Group ;
    
// 	// create the M_Expr which will reside in the group
// 	M_EXPR * MExpr = new M_EXPR(Expr,GrpID);
// 	int PrevGID;
    
// 	// find duplicate.  Done only for logical, not physical, expressions.
// 	if(MExpr -> GetOp() -> is_logical()) {
// 		M_EXPR * DupMExpr  = FindDup(*MExpr);
// 		if(DupMExpr != null)		// not null ,there is a duplicate
// 		{
// 			if (!ForGlobalEpsPruning) OptStat->DupMExpr ++;		// calculate dup mexpr
// 			PTRACE0("duplicate mexpr : " + MExpr->LightDump());
			
// 			// the duplicate is in the group the expr wanted to copyin
// 			if(GrpID == DupMExpr->GetGrpID())
// 			{	
// 				delete MExpr;
// 				return null;
// 			}
			
// 			// If the Mexpr is supposed to be in a new group, set the group id 
// 			if(GrpID == NEW_GRPID)
// 			{
// 				GrpID = DupMExpr->GetGrpID();
				
// 				// because the NewGrpID increases when constructing 
// 				// an M_EXPR with NEW_GRPID, we need to decrease it
// 				NewGrpID --;
				
// 				delete MExpr;
// 				return null;
// 			}
// 			else
// 			{
// 				// otherwise, i.e., GrpID != DupMExpr->GrpID
// 				// need do the merge
// 				GrpID = MergeGroups(GrpID , DupMExpr->GetGrpID());
// 				delete MExpr;
// 				return null;
// 			}
// 		}  // if(DupMExpr != null)
// 	} //If the expression is logical
	
    
// 	// no duplicate found
// 	if(GrpID == NEW_GRPID)
// 	{
// 		// create a new group
// 		Group = new GROUP(MExpr);
		
// 		// insert the new group into ssp
// 		GrpID = Group->GetGroupID();
		
// 		//XXX if(GrpID >= Groups.GetSize())	Groups.SetSize(GrpID + 1);
// 		//XXX Vassilis: should this be resize() or reserve?
// 		if (GrpID >= Groups.size())
// 			Groups.resize(GrpID + 1, 0);

// 		Groups[GrpID] = Group;
// 		PrevGID = GrpID;
		
		
// #ifdef IRPROP	
		
// 		// For the topmost group and for the groups containing the item operator and constant
// 		// operator, set the only physical property as any and bound INF
// 		if (GrpID == 0 || ((MExpr->GetOp())->is_const()) || ((MExpr->GetOp())->is_item()))
// 		{
// 			M_WINNER *MWin = new M_WINNER(1);
// 			M_WINNER::mc.SetAtGrow(GrpID,MWin);
// 		}
// 		else
// 		{
// 			KEYS * tmpKeySet;
			
// 			// get the relevant attributes from the schema for this group
// 			tmpKeySet = ((Group->GetLogProp()))->GetKeys()->Copy();
// 			int ksize = tmpKeySet->GetSize();
			
// 			M_WINNER *MWin = new M_WINNER(ksize+1);	
			
// 			for (int i=1; i<ksize+1; i++)
// 			{
// 				KEYS *MKEYS = new KEYS();
// 				MKEYS->Add(tmpKeySet->GetKey(i-1)->Copy());
// 				PHYS_PROP *Prop = new PHYS_PROP(new ORDER(sorted, MKEYS));
// 				//Prop->KeyOrder.Add(new ORDER(ascending));
// 				MWin->SetPhysProp(i, Prop);
// 			}
// 			delete tmpKeySet;
// 			M_WINNER::mc.SetAtGrow(GrpID,MWin);
// 		}
// #endif
		
// 	}
// 	else 
// 	{
// 		Group = GetGroup(GrpID);
		
// 		// include the new MEXPR
// 		Group->NewMExpr(MExpr);
// 	}
// 	// set the flag
// 	Group->set_changed(true);
    
// 	return MExpr;
//     } // CopyIn
    
// 	void CopyOut(int GrpID, PHYS_PROP * PhysProp, int tabs)
// 	{ 
// 		//Find the winner for this Physical Property.
// 		//print the Winner's Operator and cost
// 		GROUP * ThisGroup = Ssp -> GetGroup(GrpID);
		
// #ifndef IRPROP
// 		WINNER * ThisWinner;
// #endif
		
// 		M_EXPR * WinnerMExpr;
// 		OP * WinnerOp ;
// 		CString os;
		
// 		//special case : it's a item group
// 		if(ThisGroup->GetFirstLogMExpr()->GetOp()->is_const())
// 		{
// #ifdef IRPROP		
// 			WinnerMExpr = M_WINNER::mc[GrpID]->GetBPlan(PhysProp);
// #else
// 			WinnerMExpr = ThisGroup->GetFirstLogMExpr();
// #endif
// 			os = WinnerMExpr->GetOp()->Dump() ;
// 			os += ", Cost = 0\r\n" ;
// 			OUTPUTN(tabs, os); 
// 		}
// 		else if (ThisGroup->GetFirstLogMExpr()->GetOp()->is_item())
// 		{
// #ifdef IRPROP		
// 			WinnerMExpr = M_WINNER::mc[GrpID]->GetBPlan(PhysProp);
// 			if (WinnerMExpr == null)
// 			{
// 				os.Format("No optimal plan for group: %d with phys_prop: %s\r\n", GrpID, PhysProp->Dump());
// 				OUTPUTN(tabs, os);
// 				return;
// 			}
// #else
// 			ThisWinner = ThisGroup -> GetWinner(PhysProp);
			
// 			if (ThisWinner == null)  {
//                             os.Format("No optimal plan for group: %d with phys_prop: %s\r\n", GrpID, (const char *) PhysProp->Dump());
// 				OUTPUTN(tabs, os);
// 				return;
// 			}
			
// 			assert(ThisWinner->GetDone());
// 			WinnerMExpr = ThisWinner -> GetMPlan();
// #endif
// 			WinnerOp = WinnerMExpr -> GetOp();
			
// 			os = WinnerOp->Dump();
			
// 			os += ", Cost = " ;
			
// 			OUTPUTN(tabs, os); 
			
// #ifdef IRPROP
// 			COST * WinnerCost = M_WINNER::mc[GrpID]->GetUpperBd(PhysProp);
// #else
// 			COST * WinnerCost = ThisWinner -> GetCost();
// #endif
// 			os = WinnerCost->Dump() + "\n";
			
// 			OUTPUT0(os);
// 			PHYS_PROP *InputProp;
// 			//print the input recursively
// 			for(int i = 0; i < WinnerMExpr -> GetArity(); i++) 
// 			{
// 				InputProp = new PHYS_PROP(new ORDER(any));
// 				CopyOut(WinnerMExpr -> GetInput(i), InputProp, tabs+1);
// 				delete InputProp;
// 			}
// 		}
// 		else	// normal case
// 		{
// #ifndef IRPROP
//                     ThisWinner = ThisGroup -> GetWinner(PhysProp);
			
//                     if(ThisWinner == null)  {
//                         os.Format("No optimal plan for group: %d with phys_prop: %s\r\n", GrpID, (const char *) PhysProp->Dump());
// 				OUTPUTN(tabs, os);
// 				return;
// 			}
			
// 			assert(ThisWinner->GetDone());
// 			WinnerMExpr = ThisWinner -> GetMPlan();
// 			if (WinnerMExpr == null) {
// 				os.Format("No optimal plan for group: %d with phys_prop: %s\r\n", GrpID, (const char *) PhysProp->Dump());
// 				OUTPUTN(tabs, os);
// 				return;
// 			}
			
// #else
// 			WinnerMExpr = M_WINNER::mc[GrpID]->GetBPlan(PhysProp);
			
// #endif
			
// 			WinnerOp = WinnerMExpr -> GetOp();
// 			os = WinnerOp->Dump();
// 			if(WinnerOp->GetName()=="QSORT") os += PhysProp->Dump();
			
// 			os += ", Cost = " ;
// #ifndef _TABLE_
// 			OUTPUTN(tabs, os); 
// #endif
// #ifndef IRPROP
// 			COST * WinnerCost = ThisWinner -> GetCost();
// #else
// 			COST * WinnerCost = M_WINNER::mc[GrpID]->GetUpperBd(PhysProp);
// #endif
			
// 			os = WinnerCost->Dump() + "\n";
// #ifndef _TABLE_
// 			OUTPUT0(os);
// #else
// 			OUTPUT0("\t" + WinnerCost->Dump() + "\n");
// #endif
			
// 			//Recursively print inputs
// #ifndef _TABLE_
// 			int Arity = WinnerOp -> GetArity();
// 			PHYS_PROP * ReqProp;
// 			bool possible;
// 			for(int i = 0; i < Arity ; i++) 
// 			{
// 				int input_groupno = WinnerMExpr -> GetInput(i);
				
// 				ReqProp = ((PHYS_OP*)WinnerOp) -> InputReqdProp(PhysProp, 
// 					Ssp->GetGroup(input_groupno)->GetLogProp(),
// 					i, possible);
				
// 				assert(possible); //Otherwise optimization fails
				
// 				CopyOut(input_groupno, ReqProp, tabs+1);
				
// 				delete ReqProp ;  
// 			}
// #endif
// 		}
// 	} //CopyOut()
	
// 	// Return all the logical expression with no pruning. 
// 	vector<EXPR*> * GetExprs(int GrpID) {
// 		return AllExprs(GrpID, 0, true);
// 	};
	
// 	// Return all the physical plans with no pruning. 
// 	vector<EXPR*> * GetPlans(int GrpID){
// 		return AllExprs(GrpID, new PHYS_PROP(new ORDER(any)), false);
// 	};
	
// 	// Return all the logical expressions or physical plans with no pruning.
// 	//	Logical expressions if forLogical == true
// 	//	Physical plans if forLogical == false
// 	vector<EXPR*> * AllExprs(int GrpID, PHYS_PROP * PhysProp, bool forLogical)
// 	{
// 		vector<EXPR*> * Exprs = new vector<EXPR*>; 
// 		GROUP * ThisGroup = Ssp -> GetGroup(GrpID);   
// 		M_EXPR * MExpr;
		
// 		if (forLogical) MExpr = ThisGroup->GetFirstLogMExpr();
// 		else MExpr = ThisGroup->GetFirstPhysMExpr();
		
// 		while (MExpr) // Traverse the MExpr list
// 		{
// 			if (MExpr->GetOp()->GetName()=="QSORT" && GrpID == MExpr->GetInput(0))
// 			{ // if MExpr contains a QSORT
// 				MExpr = MExpr->GetNextMExpr();
// 				continue;
// 			};
// 			bool possible;
// 			GROUP * SubGroup;
// 			PHYS_PROP * SubProp; 
// 			OP * op = MExpr -> GetOp();
			
// 			if(!forLogical) ((PHYS_OP*)op)->SetGrpID(GrpID); // Set the GrpID for this operator (see OP for GrpID)
			
// 			vector<EXPR*> * SubExprs=null;
// 			vector<EXPR*> * SubExprs1=null;
			
// 			// If the MExpr has no input
// 			if (MExpr->GetArity()==0) 
// 				Exprs->push_back(new EXPR(op->Copy()));
			
// 			// Get the expressions from the first input group,
// 			// if the M_Expr has at least one input group
// 			// Derive the expressions, when the M_Expr has one input group
// 			if (MExpr->GetArity()){ 
// 				SubGroup = Ssp -> GetGroup(MExpr->GetInput(0));
// 				if (forLogical) // For logical expressions
// 					SubExprs= Ssp->AllExprs(SubGroup->GetGroupID(), 0, true);
// 				else { // For physical plans
// 					SubProp = ((PHYS_OP *)op)->InputReqdProp(PhysProp, SubGroup->GetLogProp(), 0, possible);
// 					if (possible)
// 						SubExprs= Ssp->AllExprs(SubGroup->GetGroupID(), SubProp, false);
// 				};
// 			};
			
// 			// Derive the expressions, when the M_Expr has one input group
// 			if (MExpr->GetArity()==1 && SubExprs) { // If it has one input groups
// 				for (uint i = 0; i<SubExprs->size(); i++)
// 					Exprs->push_back(new EXPR(op->Copy(), 
// 						   (*SubExprs)[i]));
// 			};
			
// 			// Derive the expressions , when the M_Expr has two input group
// 			if (MExpr->GetArity()==2 && SubExprs) {
// 				SubGroup = Ssp -> GetGroup(MExpr->GetInput(1));
// 				if (forLogical)
// 					SubExprs1= Ssp->AllExprs(SubGroup->GetGroupID(), 0, true);
// 				else {
// 					SubProp = ((PHYS_OP *)op)->InputReqdProp(PhysProp, SubGroup->GetLogProp(), 1, possible);
// 					if (possible)
// 						SubExprs1 = Ssp->AllExprs(SubGroup->GetGroupID(), SubProp, false);
// 				};
// 				if (SubExprs && SubExprs1){
// 					for (int i = 0; i<SubExprs->size(); i++)
// 						for (int j = 0; j <SubExprs1->size(); j++)
// 							Exprs->push_back(new EXPR(op->Copy(), (*SubExprs)[i], (*SubExprs1)[j]));
// 				};
// 			};
// 			MExpr = MExpr->GetNextMExpr();
// 		};
// 		return Exprs;
// 	} //AllExprs()
		
// 	// Space_Plans_Costs(), can print the following
// 	//		- group content
// 	//		- expressions
// 	//		- plans
// 	//		- cost distribution
// 	void Space_Plans_Costs()
// 	{
// 		// Parse Config.txt for Catalog Parameters
// 		int PrintExps;		// printing logical expressions?
// 		int SortPlans;		// 1 - sort, 0 - not sort
// 		int PrintPlans;		// printing physical plans?
// 		int PrintCats;
// 		//int PrintCosts;		// cost distribution	 
// 		int PrintGroups;	// group contents
// 		int PrintAllGroups;	// all the groups for various catalogs
// 		int PrintSortedCosts;	
// 		int FirstPlan;	// The first plan to be exported to the java file
// 		int NumPlans;		// The number of plans whose costs to be displayed 
// 		int PerJavaFile;	// The number of plans per java file
		
// 		FILE *fp;		// file handle
// 		char TextLine[LINEWIDTH]; // text line buffer
// 		char *p;
		
// 		extern CString AppDir; // global.h
// 		if((fp = fopen("Config/config.txt","r"))==null) 
// 			OUTPUT_ERROR("can not open config.txt file");
		
// 		for(;;)
// 		{
// 			fgets(TextLine,LINEWIDTH,fp);
// 			if(feof(fp)) break;
			
// 			// skip the comment line
// 			if(IsCommentOrBlankLine(TextLine)) continue;
			
// 			p = SkipSpace(TextLine);
			
// 			READ_IN ("PrintExps", PrintExps);
// 			READ_IN ("SortPlans" , SortPlans);
// 			READ_IN ("PrintPlans" , PrintPlans);
// 			READ_IN ("PrintGroups" , PrintGroups);
// 			READ_IN ("PrintAllGroups" , PrintAllGroups);
// 			READ_IN ("PrintCats", PrintCats);
// 			READ_IN ("PrintSortedCosts", PrintSortedCosts);
// 			READ_IN ("FirstPlan", FirstPlan); 
// 			READ_IN ("NumPlans", NumPlans); 
// 			READ_IN ("PerJavaFile", PerJavaFile); 
// 		}	// end of parsing loop
// 		fclose(fp);
		
// 		// Print Search Space Statistics
// 		OUTPUT0("========  Search Space Statistics =========\n");
// 		int i;
		
// 		// SSP GROUP CONTENT
// 		for (i=0; PrintGroups && i<Ssp->GetGroups()->size();i++)
// 		{
// 			OUTPUT0(Ssp->GetGroup(i)->Dump());
// 		};
		
// 		// LOGICAL EXPRESSIONS
// 		vector<EXPR *> *Exprs= Ssp->GetExprs();
// 		OUTPUT("LOGICAL Expression Number: %d \r\n", Exprs->size()); 
// 		for (i = 0; PrintExps && i<Exprs->size(); i++) {
// 			OUTPUT("%d    ", i);
// 			OUTPUT0((*Exprs)[i]->Dump());
// 			OUTPUT0("\n");
// 		}
// 		OUTPUT0("\n");
		
// 		// PHYSICAL PLANS
// 		EXPRS *Plans= new EXPRS(Ssp ->GetPlans());
// 		OUTPUT("PHYSICAL plans number %d \r\n", Plans->size()); 		
		
// 		// Print the catalogs
// 		for(i=0; PrintCats && i<CAT::Cats->size(); i++){
// 			// Populate logical property for groups
// 			CAT::Cat = (*CAT::Cats)[i];
// 			// Print the catalog 
// 			OUTPUT("[%d] ",i);		
// 			OUTPUT0(CAT::Cat->Dump() + "\n");
// 		}


// 		for(i=0; i<CAT::Cats->size(); i++){
// 			// Populate logical property for groups
// 			CAT::Cat = (*CAT::Cats)[i];
			
// 			//if(i==(CAT::Cats->GetSize()/2)) OUTPUT("%s", "\r\n");
// 			// Clean all the log prop for all the groups
// 			for (uint ii=0; ii<Groups.size(); ii++){
// 				Groups[ii]->SetLogProp(null);
// 			}
// 			for (uint ii=0; ii<Groups.size(); ii++){
// 				// Recompute LogProp, and update GET::Coll 
// 				Groups[ii]->CatalogUpdate();	
// 			}
		
// 			// SSP GROUP CONTENT for this catalog
// 			for (uint ii=0; PrintAllGroups && ii<Ssp->GetGroups()->size();ii++)
// 			{
// 				OUTPUT0(Ssp->GetGroup(ii)->Dump());
// 			};
			
// 			// Print, for each catalog,the costs of all the plans
// 			OUTPUT("%s", "\r\n");
// 			for(uint iii=0;iii<Plans->size(); iii++)	OUTPUT("P%d\t", iii);
// 			OUTPUT("%s", "\r\n");
			
// 			// Dump the plan cost for the particular catalog
// 			//CString temp;
// 			//temp.Format("Costs [%d]:\t", i);
// 			//DOUBLES *CostVals = new DOUBLES;
// 			//for (ii = 0; ii<Plans->GetSize(); ii++) {
// 			//	double cost=Plans->ElementAt(ii)->GetCost()->getValue();					
// 				// Print out the costs of the plan
// 			//	if (PrintSortedCosts) {CostVals->Add(cost);} else {CostVals->Append(cost);}; 
// 			//}
// 			//OUTPUT("%s\r\n", CostVals->Dump());
// 			// If necessary, dump the java file
// 		}

// 		// Generate java plans, Note that it uses the last catalog if catalogs vary
// 		for (int ii = 0; ii<Plans->size(); ii++) {
// 			double costVal = Plans->GetAt(ii)->GetCost()->getValue();					
// 			// Sort the plan in ascending order 
// 			EXPR * plan = Plans->GetAt(ii); 
// 			plan->SetOriginalPlanNo(ii);
// 			plan->SetCostVal(costVal);
// 		}
		
// 		// Sort the plans if the SortPlan flag is 1
// 		if (SortPlans ==1) {
// 			EXPRS * SortedPlans = new EXPRS; 
// 			for (int ii = 0; ii<Plans->size(); ii++) {
// 				EXPR * plan = Plans->GetAt(ii); 
// 				SortedPlans->Add(plan);	// Sort the plan in ascending order 

// 			}
// 			Plans = SortedPlans;
// 		}
	
// 		if (Plans->size()>FirstPlan){ 
// 			// XXX Vassilis: let's try this...: if (FirstPlan>0) Plans->RemoveAt(0, FirstPlan);
// 			if (FirstPlan>0) Plans->erase(Plans->begin(), 
// 					Plans->begin()+FirstPlan); // XXX Vassilis Maybe +1?
// 		}
// 		// Dump AT MOST 100 PLANS ARE INCLUDED, limited by the java method size 
// 		if (Plans->size()>NumPlans){ 
// 			// XXX Vassilis: let's try this...: Plans->RemoveAt(NumPlans, Plans->size()-NumPlans);
// 			Plans->erase(Plans->begin()+NumPlans, Plans->end());
// 		}

// 		//OUTPUT("Original Plan numbers    %s\r\n","");
// 		//for (i = 0; i<SortedPlans->GetSize(); i++) {
// 		//	OUTPUT("%d\t", SortedPlans->ElementAt(i)->GetOriginalPlanNo());
// 		//}
// 		//OUTPUT("%s", "\r\n");

// 		OUTPUT("Cost Model    %s\r\n","");
// 		OUTPUT0(Cm->Dump()); 
// 		OUTPUT("%s", "\r\n");

// 		// Output costs to screen and tbe "datat\costs.txt" file
// 		CString costs="";
// 		for (i = 0; i<Plans->size(); i++) {
// 			CString temp;
// 			temp.Format("%.0f\t", Plans->GetAt(i)->GetCostVal());
// 			costs +=temp;
// 		}
// 		OUTPUT("%s", "\r\n");

//                 CString costsFileName = UI::getAppDir() + "/data/costsout.txt";
//                 //XXX Vassilis this should be moved somewhere else...
//                 ofstream CostsFile(costsFileName);
//                 if (!CostsFile.is_open()) {
//                     UI::error("Could not open file: " + costsFileName, 
//                               __FILE__, __LINE__);
//                     abort();
//                 }
//                 CostsFile << costs;
//                 CostsFile.close();

// 		OUTPUT("Plan Costs    %s\r\n","");
// 		OUTPUT0(costs + "\n"); 


// 		OUTPUT("Plan Kinds: %s","0 - unnested; 1 - partially unnested; 2-nested\r\n");
// 		for (i = 0; i<Plans->size(); i++){
// 			OUTPUT("%d\t ", Plans->GetAt(i)->PlanKind());
// 		}
// 		OUTPUT("%s", "\r\n");

// 		OUTPUT("Original Plan Numbers: %s","\r\n");
// 		for (i = 0; i<Plans->size(); i++){
// 			OUTPUT("%d\t ", Plans->GetAt(i)->GetOriginalPlanNo());
// 		}
// 		OUTPUT("%s", "\r\n");
		
// 		OUTPUT("Plans starting at Plan %d\r\n",FirstPlan);
// 		for (i = 0; PrintPlans && i<Plans->size(); i++) {
// 			OUTPUT("%d    ",i+FirstPlan);
// 			OUTPUT0(Plans->GetAt(i)->Dump() + " ");
// 			OUTPUT("%s", "\r\n");
// 		}

// 		for(i=0; i*PerJavaFile < Plans->size(); i++){
//                     CString PlanFileName;
//                     PlanFileName.Format("%s/Evaluator/Query%d_%d_%d_%d.%d", 
//                                         UI::getAppDir().c_str(),
// 				CAT::QueryToRun, CAT::Cat->DeptCard, CAT::Cat->StdFanin, CAT::Cat->EmpFanin,i); 
//                         ofstream JavaPlans(PlanFileName);
//                         if (!JavaPlans.is_open()) {
//                             UI::error("Could not open file: " + PlanFileName, 
//                                       __FILE__, __LINE__);
// 			    abort();
// 			}
// 			CString javaplans = Plans->DumpJava(CAT::Cat->DeptCard, 
//                                                             CAT::Cat->StdFanin, 
//                                                             CAT::Cat->EmpFanin, 
//                                                             CAT::Cat->Density, 
//                                                             FirstPlan+i*PerJavaFile, // the starting plan 
//                                                             PerJavaFile);	// the number of plans
//                         JavaPlans << javaplans;
// 			JavaPlans.close();
// 			//OUTPUT("%s", javaplans); 
// 		}
// 	}

// 	int TaskNo=0;
	
//     void optimize()
//     {
//         std::cerr << "Entering optimize, CONT::vc.size() = " << CONT::vc.size() << std::endl;

// #ifdef FIRSTPLAN
// 		Ssp -> GetGroup(0) -> setfirstplan(false);
// #endif

		
// 		SET_TRACE Trace(true);
		
// 		//Create initial context, with no requested properties, infinite upper bound,
// 		// zero lower bound, not yet done.  Later this may be specified by user.
// 		if (CONT::vc.size() == 0)
// 		{
// 			CONT * InitCont = new CONT(new PHYS_PROP(new ORDER(any)), new COST(-1), false);
// 			//Make this the first context
// 			CONT::vc.push_back(InitCont);
// 		}
//         std::cerr << "In optimize, right before assertion, size = " << CONT::vc.size() << std::endl;
// 		assert(CONT::vc.size() == 1); 
		
// 		// start optimization with root group, 0th context, parent task of zero.  
// 		if (GlobepsPruning)
// 		{
// 			COST * eps_bound = new COST(GlobalEpsBound);
// 			PTasks.push (new O_GROUP (RootGID, 0, 0, true, eps_bound));
// 		}
// 		else
// 			PTasks.push (new O_GROUP (RootGID, 0, 0));
		
// 		PTRACE ("initial OPEN:\r\n %s\r\n", 
//                         (const char *) PTasks.Dump());
		
// 		// main loop of optimization
// 		// while there are tasks undone, do one
// 		while (! PTasks.empty ())
// 		{
// 			TaskNo ++;
// 			PTRACE ("Starting task %d", TaskNo);
			
// 			TASK * NextTask = PTasks.pop ();
// 			NextTask -> perform ();
			
// 			if(TraceSSP) 
// 			{ 
// 				TRACE_FILE("\r\n====== SSP after task %d: ", TaskNo);
// 				TRACE_FILE0(DumpChanged() + "\n"); 
// 			}
// 			else { 
//                             PTRACE0(DumpChanged()); 
//                         }
			
// 			if(TraceOPEN) {
// 				TRACE_FILE("\r\n====== OPEN after task %d:\r\n", TaskNo);
// 				TRACE_FILE0(PTasks.Dump() + "\n");
// 			}
// 			else {	
//                             PTRACE("OPEN after task %d:\n", TaskNo);
//                             PTRACE0(PTasks.Dump() + "\n"); 
//                         }
// 		} // main optimization loop over remaining tasks in task list
		
// 		PTRACE ("Optimizing completed: %d tasks\r\n", TaskNo);
// #ifdef _TABLE_
// 		OUTPUT("%s\t", GlobalEpsBound.Dump());
// 		OUTPUT("%d\t", ClassStat[C_M_EXPR]->Count);
// 		OUTPUT("%d\t", ClassStat[C_M_EXPR]->Total);
// 		OUTPUT("%d\t", TaskNo);
// #else
// 		OUTPUT("TotalTask : %d\r\n", TaskNo);
// 		OUTPUT("TotalGroup : %d\r\n", ClassStat[C_GROUP]->Count);
// 		OUTPUT("CurrentMExpr : %d\r\n", ClassStat[C_M_EXPR]->Count);
// 		OUTPUT("TotalMExpr : %d\r\n", ClassStat[C_M_EXPR]->Total);
// 		OUTPUT0(OptStat->Dump());
// #endif
//     }  

    public int getRulesetSize() {
	return rulesetSize;
    }
}
