
/**********************************************************************
  $Id: QueryThread.java,v 1.4 2002/05/23 06:31:41 vpapad Exp $


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


package niagara.query_engine;

import java.util.Vector;
import java.io.FileReader;
import java.io.File;
import java.io.StringReader;
import niagara.data_manager.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.*;

/**
 *  The QueryThread class is used to run queries after they are entered 
 *  into the query engine system through executeQuery(String).  Each query
 *  thread does the following:
 *  
 *  <ul>
 *  <li>Parse the query producing a logical operator tree
 *  <li>Give this tree to the Optimizer, which produces an optimized tree
 *  <li>Instatiate this tree by creating PhysicalOperators for each logical node
 *  <li>Give this physical operator tree to the scheduler which places ops into 
 *      the PhysicalOperatorBuffer.
 *  <li>PhysicalOperatorThreads are primed, and consume and run these Physical 
 *      Operators.
 *  </ul>
 *
 *
 *  @see QueryOptimizer
 *  @see ExecutionScheduler
 *  @see PhysicalOperator
 *  @see PhysicalOperatorThread
 *  @see QueryParser
 *  @see QueryEngine
 */

public class QueryThread implements Runnable {
	
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //   Data members of the QueryThread Class
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    // The thread associated with the class
    //
    private Thread thread;

    // The query queue on which QueryThread is to wait, shared by all threads
    //
    private QueryQueue queryQueue;

    // The query scheduler, shared by all query threads
    //
    private ExecutionScheduler scheduler;

    // Parser for parsing xml-ql, 1 per thread
    //
    private QueryParser queryParser;

    // Logical plan generator from parse tree, 1 per thread
    //
    private logPlanGenerator logicalPlanGenerator;

    // The query optimizer, 1 per thread
    //
    private QueryOptimizer queryOptimizer;


    
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //   Methods of the Query Class
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    /**
     * Constructor for QueryThread class.
     * 
     * Initialized with an QueryQueue to block on, and  an ExecutionScheduler.
     * A Java thread is created and is associated with the Query Thread.
     *
     * @param dataManager the data manager (common to query engine)
     * @param queryQueue  the name of the query queue with which the
     * @param scheduler  the execution scheduler (shared by all query threads)
     */  

    public QueryThread (DataManager dataManager,
						QueryQueue queryQueue,
						ExecutionScheduler scheduler) {

		// Initialize the reference to the query queue
		//
		this.queryQueue = queryQueue;
	
		// Initialize the reference to the scheduler
		//
		this.scheduler = scheduler;

		// Create a parser for this thread
		// CANNOT DO NOW
		// queryParser = new QueryParser();

		// Create a logical plan generator for this thread
		// CANNOT DO NOW
		// logicalPlanGenerator = new logPlanGenerator();

		// Create an optimizer for this thread
		//
		queryOptimizer = new QueryOptimizer(dataManager);

		// Create a new java thread for running an instance of this object
		//
		thread = new Thread (this,"QueryThread");

		// Call the query thread run method
		//
		thread.start();	

		return;
    }
		     

    /**
     *  This is the run method invoked by the Java thread - it simply grabs 
     *  the next query, executes it, and then repeats.
     */

    public void run () {

	    // Waiting on the Query Queue until there is a new query to
	    // be scheduled. Then once an query is obtained, run it to completion.
	    // Then repeat the process.
	do {
	    try {
		// Get a query
		QueryInfo queryInfo = queryQueue.getQuery();
		
		// Execute it
		execute(queryInfo);
	    } catch(UserErrorException uee) {
		// should pass this back to client, for now, just print
		System.err.println("USER ERROR: " + uee.getMessage());
	    } catch (ShutdownException se) {
		// do nothing - just go to next query, is this correct?
		System.err.println("QueryThread got Shutdown for current query");
	    }
	} while (true);
    }


    /**
     *  This function executes a query
     *  
     *  @param queryInfo the QueryInfo object that contains information about
     *                   the query to be run
     */

    private void execute (QueryInfo queryInfo)
    throws UserErrorException, ShutdownException {

		// Get the string version of the query
		String queryString = queryInfo.getQueryString();

		// Create a scanner and query parser on the fly
		// THIS HAS TO CHANGE TO REUSE SCANNER AND PARSER
		Scanner scanner;

		//try {
		    scanner = new Scanner(new EscapedUnicodeReader(
				    new StringReader(queryString)));
		    //} catch (Exception e) {
		    // KT FIX - need to do better error checking than this!!
		    //System.err.println("Problem with the query string "+queryString);
		    //queryInfo.killQueryWithoutOperators();
		    //return;
		    //}

		queryParser = new QueryParser(scanner);

		// Get the parse tree
		//
		java_cup.runtime.Symbol parseTree;

		try {
		    parseTree = queryParser.parse();
		} catch (Exception e) { // this is what cup throws
		    queryInfo.killQueryWithoutOperators();
		    throw new UserErrorException("Error parsing query");
		}

		// Get the query representation from the parse tree
		//
		query queryRep = ((xqlExt) parseTree.value).getQuery();

		// Get the logical plan from the query representation
		// THIS HAS TO CHANGE TO REUSE LOGICAL PLAN GENERATOR
		//
		logicalPlanGenerator = new logPlanGenerator(queryRep);

		// Get the logical plan
		//
		logNode logicalPlan = logicalPlanGenerator.getLogPlan();

		// Perform optimization on the logical plan and get optimized plan
		//

		logNode optimizedPlan = null;
		try {
		        optimizedPlan = queryOptimizer.optimize(logicalPlan);
		}
		catch (NoDataSourceException e) {
			System.out.println("No Valid URLs returned");
			queryInfo.killQueryWithoutOperators();
			return;
		}


		// If there was an error, then exit
		//
		if (optimizedPlan == null) {
			System.err.println("Error in Optimizing Query");
			queryInfo.killQueryWithoutOperators();
			return;
		}

		// Send the optimized plan to scheduler for execution
		//
		scheduler.executeOperators(optimizedPlan, queryInfo);
    }
}


