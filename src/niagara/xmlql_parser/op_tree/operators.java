
/**********************************************************************
  $Id: operators.java,v 1.6 2001/07/17 06:52:23 vpapad Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


/**
*
* This class stores an object of all the different logical operators which
* can be cloned to build a logical operator tree. An object of each operator
* is initialised with the list of class names that represent the algorithms to
* implement them.
*
*
*/

package niagara.xmlql_parser.op_tree;

import java.util.*;
import java.lang.*;

public class operators {
    private static Hashtable operatorNames = new Hashtable();

	public static dbScanOp DbScan;     // to read a database (not used)
	public static dtdScanOp DtdScan;   // to read the XML data sources
    public static FirehoseScanOp FirehoseScan;   // to read from a firehose
    public static ConstantOp constantOp;   // embedded XML document
    public static ResourceOp resourceOp;   // an abstract resource
        public static tupleScanOp TupleScan; //Trigger
	public static scanOp Scan;         // to read the elements
	public static selectOp Select;     // Selection
	public static joinOp Join;         // Join two stream
	public static constructOp Construct; // constructing XML results
    	public static nestOp Nest;
        public static averageOp Average;
    public static SumOp Sum;
    public static CountOp Count;
        public static dupOp Duplicate; //Trigger
        public static splitOp Split; //Trigger
        public static trigActionOp TrigAct; //Trigger
    public static AccumulateOp Accumulate; // To accumulate a stream
    public static ExpressionOp expression; // To calculate an arbitrary expression
    public static SortOp sort; // Sorting
    public static UnionOp union; // Union of streams

    public static DisplayOp display; // Sending results to client

    public static SendOp send; // Sending subplan results
    public static ReceiveOp receive; // Receiving subplan results

// Names of the classes that implement the algorithms for different operators

	private static String[] dbScanAlgo = {"niagara.query_engine.PhysicalScanOperator"};
	private static String[] dtdScanAlgo = {};
    private static String[] firehoseScanAlgo = {}; 
    private static String[] constantOpAlgo = {}; 
    private static String[] resourceOpAlgo = {}; 
        private static String[] tupleScanAlgo = {};
	private static String[] scanAlgo = {"niagara.query_engine.PhysicalScanOperator"};
	private static String[] selectAlgo = {"niagara.query_engine.PhysicalSelectOperator"};
	private static String[] joinAlgo = {"niagara.query_engine.PhysicalNLJoinOperator", 
					    "niagara.query_engine.PhysicalHashJoinOperator"};
	private static String[] constructAlgo = {"niagara.query_engine.PhysicalConstructOperator"};
	private static String[] nestAlgo = {"niagara.query_engine.PhysicalNestOperator"};
        private static String[] averageAlgo = {"niagara.query_engine.PhysicalAverageOperator"};
    private static String[] accumulateAlgo = {"niagara.query_engine.PhysicalAccumulateOperator"}; 
    private static String[] expressionAlgo = {"niagara.query_engine.PhysicalExpressionOperator"}; 
    private static String[] sortAlgo = {"niagara.query_engine.PhysicalSortOperator"}; 
    private static String[] unionAlgo = {"niagara.query_engine.PhysicalUnionOperator"}; 

    private static String[] displayAlgo = {"niagara.query_engine.PhysicalDisplayOperator"}; 
    private static String[] sendAlgo = {"niagara.query_engine.PhysicalSendOperator"}; 
    private static String[] receiveAlgo = {}; 

        private static String[] sumAlgo = {"niagara.query_engine.PhysicalSumOperator"};
        private static String[] countAlgo = {"niagara.query_engine.PhysicalCountOperator"};
        private static String[] duplicateAlgo = {"niagara.query_engine.PhysicalDuplicateOperator"};
        private static String[] splitAlgo = {"niagara.query_engine.PhysicalSplitOperator"};
        private static String[] trigActAlgo =
        {"niagara.query_engine.PhysicalTrigActionOperator"};

// each static operator is initialised
	static {
	    try{		
		int numOfAlgo;
		Class[] algoClasses;
		
		// DbScan
		numOfAlgo = dbScanAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(dbScanAlgo[i]);
		DbScan = new dbScanOp(algoClasses);

		// dtdScan
		numOfAlgo = dtdScanAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(dtdScanAlgo[i]);
		DtdScan = new dtdScanOp(algoClasses);
                operatorNames.put(DtdScan.getClass().getName(), "dtdscan");

		// FirehoseScan
		numOfAlgo = firehoseScanAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(firehoseScanAlgo[i]);
		FirehoseScan = new FirehoseScanOp(algoClasses);
                operatorNames.put(FirehoseScan.getClass().getName(), "fhscan");

		// ConstantOp
		numOfAlgo = constantOpAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(constantOpAlgo[i]);
		constantOp = new ConstantOp(algoClasses);
                operatorNames.put(constantOp.getClass().getName(), "constant");

		// ResourceOp
		numOfAlgo = resourceOpAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(resourceOpAlgo[i]);
		resourceOp = new ResourceOp(algoClasses);
                operatorNames.put(resourceOp.getClass().getName(), "resource");

		// TupleScan
		numOfAlgo = tupleScanAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(tupleScanAlgo[i]);
		TupleScan = new tupleScanOp(algoClasses);

		// Scan
		numOfAlgo = scanAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(scanAlgo[i]);
		Scan = new scanOp(algoClasses);
                operatorNames.put(Scan.getClass().getName(), "scan");

		// Select
		numOfAlgo = selectAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(selectAlgo[i]);
		Select = new selectOp(algoClasses);
                operatorNames.put(Select.getClass().getName(), "select");

		// Join
		numOfAlgo = joinAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(joinAlgo[i]);
		Join = new joinOp(algoClasses);
                operatorNames.put(Join.getClass().getName(), "join");

		// Construct
		numOfAlgo = constructAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(constructAlgo[i]);
		Construct = new constructOp(algoClasses);
                operatorNames.put(Construct.getClass().getName(), "construct");
	
		// Nest
		numOfAlgo = nestAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(nestAlgo[i]);
		Nest = new nestOp(algoClasses);

		// Average
		numOfAlgo = averageAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(averageAlgo[i]);
		Average = new averageOp(algoClasses);
                operatorNames.put(Average.getClass().getName(), "average");

		// Accumulate
		numOfAlgo = accumulateAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(accumulateAlgo[i]);
		Accumulate = new AccumulateOp(algoClasses);
                operatorNames.put(Accumulate.getClass().getName(), "accumulate");

		// Expression
		numOfAlgo = expressionAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(expressionAlgo[i]);
		expression = new ExpressionOp(algoClasses);
                operatorNames.put(expression.getClass().getName(), "expression");

		// Sort
		numOfAlgo = sortAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(sortAlgo[i]);
		sort = new SortOp(algoClasses);
                operatorNames.put(sort.getClass().getName(), "sort");

		// Union
		numOfAlgo = unionAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(unionAlgo[i]);
		union = new UnionOp(algoClasses);
                operatorNames.put(union.getClass().getName(), "union");

		// Display
		numOfAlgo = displayAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(displayAlgo[i]);
		display = new DisplayOp(algoClasses);
                operatorNames.put(display.getClass().getName(), "display");

		// Send
		numOfAlgo = sendAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(sendAlgo[i]);
		send = new SendOp(algoClasses);
                operatorNames.put(send.getClass().getName(), "send");

		// Receive
		numOfAlgo = receiveAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(receiveAlgo[i]);
		receive = new ReceiveOp(algoClasses);
                operatorNames.put(receive.getClass().getName(), "receive");

		// Sum
		numOfAlgo = sumAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(sumAlgo[i]);
		Sum = new SumOp(algoClasses);
                operatorNames.put(Sum.getClass().getName(), "sum");

		// Count
		numOfAlgo = countAlgo.length;
		algoClasses = new Class[numOfAlgo];
		for(int i=0;i<numOfAlgo;i++)
			algoClasses[i] = Class.forName(countAlgo[i]);
		Count = new CountOp(algoClasses);
                operatorNames.put(Count.getClass().getName(), "count");

		// Duplicate
                numOfAlgo = duplicateAlgo.length;
                algoClasses = new Class[numOfAlgo];
                for(int i=0;i<numOfAlgo;i++)
                        algoClasses[i] = Class.forName(duplicateAlgo[i]);
                Duplicate = new dupOp(algoClasses);
                operatorNames.put(Duplicate.getClass().getName(), "dup");

		// Split
                numOfAlgo = splitAlgo.length;
                algoClasses = new Class[numOfAlgo];
                for(int i=0;i<numOfAlgo;i++)
                        algoClasses[i] = Class.forName(splitAlgo[i]);
                Split = new splitOp(algoClasses);

		// TrigAct
                numOfAlgo = trigActAlgo.length;
                algoClasses = new Class[numOfAlgo];
                for(int i=0;i<numOfAlgo;i++)
                        algoClasses[i] = Class.forName(trigActAlgo[i]);
                TrigAct = new trigActionOp(algoClasses);
                
	      } catch (ClassNotFoundException e) {
			System.err.println(e);	
	      }	
	}

    public static String getName(Object o) {
        if (operatorNames.containsKey(o.getClass().getName())) 
            return (String) operatorNames.get(o.getClass().getName());
        else
            return "INVALIDOP";
    }
}
