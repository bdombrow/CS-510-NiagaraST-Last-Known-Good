/**
 * $Id: Optimizer.java,v 1.6 2002/07/05 22:25:48 vpapad Exp $
 */
package niagara.optimizer;

import niagara.xmlql_parser.op_tree.logNode;
import niagara.connection_server.NiagraServer;
import niagara.connection_server.Catalog;

//import niagara.optimizer.colombia.SSP;

 public class Optimizer {
    //private static SSP ssp; // Search space

    public static void init() {
	System.out.println("XXX vpapad: in initColumbia!");
//	ssp = new SSP();

// 	initStatistics();
	
// 	cerr << "XXX vpapad here 1" << endl;
// 	// Initialize cost model
// 	Cm = new CM(env, catalog);
// 	PTRACE0("cost model content:\n" + Cm->Dump());
// 	cerr << "XXX vpapad here 2" << endl;
// 	//Generate various catalogs
// 	CAT::PopulateCats(env, catalog);
// 	PTRACE0("Catalog content:\n" + CAT::Cat->Dump());
    }


    public static logNode optimize(logNode plan) {
	System.out.println("XXX vpapad: in optimize!");
//     IntOrdersSet = 0; // XXX vpapad let's see what breaks after this!
// 	SET_TRACE Trace(true);
	
// //        UI::setOutputFile("Data/colout.txt");
	
// //        UI::createOutputCOVE();
        
	
//     // clean the statistic
//     for(int i=0; i < CLASS_NUM; i++) {
//         if (!ClassStat[i])
//             continue;
//         ClassStat[i]->Count = 0;
//         ClassStat[i]->Max = 0;
//         ClassStat[i]->Total = 0;
//     }
	
	
// 	bool first=true;
// 	// Opt statistics object
// 	OptStat = new OPT_STAT;	
	
	
	
// 	// Get the input query
// 	// Query = new QUERY(QueryFile);
// 	// Mannually Populate the query 
// 	Query = SampleQuery();

// 	// Modify the paring result 
// 	//   1. changing GET into GET_CVA if necessary
// 	//   2. remove sub-expression in operator arguments
// 	//Query = Normalize(Query); 
	
// 	// Get hueristic cost for globalEpsPruning
// 	GetGlobalEpsBound();
	
// 	Pruning=false;

// #ifdef _DEBUG
// 	oldMemState.Checkpoint();
// #endif

	//ssp.init();

// 	// print the content of the initial search space
// 	PTRACE0("Initial Search Space:\n" + Ssp->Dump());
	
// 	PTRACE0("---1--- memory statistics before optimization: " + DumpStatistics());
// 	PTRACE("used memory before opt: %dK\r\n",GetUsedMemory()/1000);
	
// 	// Load rules 
// 	if(RSFile=="") RSFile = "Config/AllRules.txt";			
// 	RuleSet = new RULE_SET(RSFile);
// 	PTRACE0("Rule set content:\n" + RuleSet->Dump());
		
// #ifdef _DEBUG
// 	//Rule Firing Statistics
// 	TopMatch.SetSize(RuleSet->RuleCount);
// 	Bindings.SetSize(RuleSet->RuleCount);
// 	Conditions.SetSize(RuleSet->RuleCount);
// 	for (int RuleNum = 0; RuleNum < RuleSet->RuleCount ; RuleNum++)
// 	{
// 		TopMatch[RuleNum] = 0;
// 		Bindings[RuleNum] = 0;
// 		Conditions[RuleNum] = 0;
// 	}
// #endif

// 	Timing(0);		// Start timing
// 	Ssp->optimize();	//Later add an input condition	
// 	Timing(1);		// Finish timing
	
// #ifdef _DEBUG
// 	OUTPUT("%s",RuleSet -> DumpStats());
// #endif

// 	PTRACE("used memory after opt: %dK\r\n",GetUsedMemory()/1000);
	
// 	// Print final search space, including optimal plan 	
// 	// Print the candidate plans and the cost distribution
// 	Ssp->Space_Plans_Costs();

	
// 	//OUTPUT("==== memory manager statistics after opt: ====\r\n%s", memory_manager->Dump());
// 	//PTRACE("%s",DumpHashTable());
	
// 	// free catalogs
//         // XXX vassilis move to class CAT
//         for (vector<CAT*>::iterator it = CAT::Cats->begin();
//              it != CAT::Cats->end();
//              it++)
//             delete *it;

// 	CAT::Cats->clear();

// 	// free each context
// 	for(uint i = 0; i < CONT::vc.size();i++) delete CONT::vc[i];
// 	CONT::vc.clear();
// 	//if (RadioVal ==0 && q==NumQuery-1) fclose(fp);
	
// 	PTRACE("used memory before delete ssp: %dM\r\n",GetUsedMemory()/1000);
// 	delete Ssp;
	//ssp.clear();
	
// 	PTRACE("used memory before delete cat: %dM\r\n",GetUsedMemory()/1000);
	
// 	PTRACE0("---3--- memory statistics after freeing searching space: "
//                 + DumpStatistics());
	
// 	delete OptStat;	// free opt statistics obj
	
// 	delete Cm;		// free cost model
	
// 	delete RuleSet;	// free rule set
	
	
// 	PTRACE("used memory after delete manager: %dM\r\n",GetUsedMemory()/1000);
	
//     UI::closeOutputFile();
//     UI::closeOutputCOVE();

	return plan;
    }

// // The catalog
// CAT * CAT::Cat;

// void Optimizer();


// // Refer to static data members
// vector<CONT*> CONT::vc;
// vector<CAT*> *CAT::Cats;
// int CAT::QueryToRun;
// int CAT::AllowCartesian;

// #define LINEWIDTH 256		// buffer length of one text line
// #define KEYWORD_NUMOFQRY "NumOfQuery:"
// #define KEYWORD_QUERY "Query:"

// #ifdef _DEBUG
// #define new DEBUG_NEW
// CMemoryState oldMemState, newMemState, diffMemState;
// #endif

// #ifdef _DEBUG
// //Rule Firing Statistics
// INT_ARRAY TopMatch;
// INT_ARRAY Bindings;
// INT_ARRAY Conditions;
// #endif

// //extern CAT * SampleCat();	// example.cpp

// QUERY * SampleQuery(); // example.cpp

// void GetGlobalEpsBound(); // Assign global epsilon bound

// #ifdef WIN32
// struct _timeb start, finish;
// #else 
// struct timeb start, finish;
// #endif 

// CString NewVarName();

// char *timeline;
// CString tmpbuf;
// void Timing(int);

// /************* 	helping functions for optimize()  ********************/

// static int newVarName=0;
// CString NewVarName()
// {
// 	CString os;
// 	os.Format("%d", newVarName);
// 	newVarName++;
// 	return os;
// }

// //
// //	Obtain the cost for globalEpsPrunning
// //
// void GetGlobalEpsBound()
// {
// 	PHYS_PROP * PhysProp;
	
// 	int i;
// 	// if GlobepsPruning, run optimizer without globepsPruning
// 	// to get the heuristic cost
// 	if(GlobepsPruning)
// 	{
// 		GlobepsPruning = false;
// 		ForGlobalEpsPruning = true;

// 		//Query = new QUERY();
// 		Query = SampleQuery();
// 		Ssp = new SSP;
// 		Ssp->Init();
// 		delete Query;
// 		Ssp->optimize();
// 		PhysProp = CONT::vc[0]->GetPhysProp();
// 		COST *HeuristicCost = new COST(0);	
// 		*HeuristicCost = *(Ssp->GetGroup(0)->GetWinner(PhysProp)->GetCost());
// 		assert(Ssp->GetGroup(0)->GetWinner(PhysProp) ->GetDone());
// 		GlobalEpsBound = (*HeuristicCost)*(GLOBAL_EPS);
// 		delete Ssp;
// 		for(uint i = 0; i < CONT::vc.size(); i++) 
//                     delete CONT::vc[i];
// 		for(uint i = 0; i < CAT::Cats->size(); i++) 
//                     delete (*CAT::Cats)[i];
// 		CONT::vc.clear();
// 		GlobepsPruning = true;
// 		ForGlobalEpsPruning = false;
// 		delete HeuristicCost;  // free Cost
// 	}
// #ifdef _TABLE_
	
// 	OUTPUT("%s", "EPS,  EPS_BD, CUREXPR, TOTEXPR,   TASKS,  OPTCOST\r\n");
// 	for (double ii = 0; ii<= GLOBAL_EPS*10; ii++)
// 	{
// 		OUTPUT("%3.1f\t", ii/10);
// 		GlobalEpsBound = (*HeuristicCost)*ii/10;
// 		ClassStat[C_M_EXPR]->Count = ClassStat[C_M_EXPR]->Total = 0;
// 	}
	
// #endif
// }

// void Timing(int start_finish)
// {
//     // XXX Vassilis make this into two functions, start and stop
// 	if (start_finish==0){

// #ifdef WIN32           
// 		_ftime(&start); // Get current time
// #else 
//                 ftime(&start);
// #endif
// 		timeline = ctime(& (start.time));
// 		tmpbuf.Format("%.8s.%0.3hu", &timeline[11], start.millitm);
		
// #ifndef _TABLE_
// 		OUTPUT_S("Optimization beginning time:\t\t%s (hr:min:sec.msec)\r\n", tmpbuf);
// #endif
		
		
// 	}
// 	if (start_finish==1){
// #ifndef _TABLE_
// 		long time;			//total seconds from start to finish
// 		unsigned short msecs;	//milliseconds from start to finish
// #ifdef WIN32
// 		_ftime(&finish);
// #else
//                 ftime(&finish);
// #endif
// 		if (finish.millitm >= start.millitm)
// 		{
// 			time = finish.time - start.time;
// 			msecs = finish.millitm - start.millitm;
// 		}
// 		else
// 		{
// 			time = finish.time - start.time - 1;
// 			msecs = 1000 + finish.millitm - start.millitm;
// 		}
// 		long hrs, mins, secs;	// Print differences from start to finish
// 		secs = time %60;
// 		mins = ((time - secs)/60) % 60;
// 		hrs = (time - secs - mins*60 - secs)/3600 ;
// 		tmpbuf.Format("%0.2d:%0.2d:%0.2d.%0.3d\r\n",hrs, mins, secs, msecs);
// 		OUTPUT_S("Optimization elapsed time:\t\t%s", tmpbuf);
// #endif
// 	}
// }

// void FinalSsp()
// {
// 	// Print final search space, including optimal plan 
// 	// always output final optimal plan
	
// #ifndef _TABLE_
// 	OUTPUT("%s","================== OPTIMAL PLAN ===============");
// #endif
	
// 	PHYS_PROP * PhysProp = CONT::vc[0]->GetPhysProp();
// 	Ssp->CopyOut(0, PhysProp, 0);
	
// 	PTRACE0("---2--- memory statistics after optimization: " + DumpStatistics());
	
// 	// Print final search space 
// 	if(TraceFinalSSP) 
// 	{ Ssp->FastDump(); }
// 	else
// 	{ PTRACE0("final Search Space:\n" + Ssp->Dump()); }
	
// }

// QUERY *  myQuery() { // Vassilis #$@#$@#$# XXX
// 	// Get ("Emps", "e")
// 	EXPR * exp1 = new EXPR(new GET("Emps", "e"));
// 	exp1 = new EXPR(new MAT("e"), exp1);

// 	EXPR * exp2 = new EXPR(new GET("Depts", "d"));
// 	exp2 = new EXPR(new MAT("d"), exp2);

// 	EXPR* exp = new EXPR(new EQJOIN(new PREDICATE("e.dept", "d", OP_EQ)), 
//                              exp1, exp2);

// 	QUERY * result = new QUERY(exp);
// 	// always output inital query
// 	PTRACE0("Original Query:\n" + result->Dump()); 	
// 	return (result);
// }


// QUERY * SampleQuery() 
// {	
// // XXX Vassilis stupid lazy code
//     return myQuery();
// }


}
