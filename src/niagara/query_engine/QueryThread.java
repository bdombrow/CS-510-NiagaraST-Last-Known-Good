/**********************************************************************
  $Id: QueryThread.java,v 1.11 2003/12/24 01:31:44 vpapad Exp $


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

import java.io.StringReader;

import niagara.connection_server.XMLQueryPlanParser;
import niagara.data_manager.*;
import niagara.xmlql_parser.*;
import niagara.logical.*;
import niagara.optimizer.Optimizer;
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
    private QueryQueue queryQueue;

    // The query scheduler, shared by all query threads
    //
    private ExecutionScheduler scheduler;

    // Parser for parsing xml-ql, 1 per thread
    private QueryParser queryParser;

    // Logical plan generator from parse tree, 1 per thread
    private LogPlanGenerator logicalPlanGenerator;

    // The query optimizer, 1 per thread
    private QueryOptimizer queryOptimizer;


    private XMLQueryPlanParser xqpp;
    private Optimizer optimizer;
    
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
		this.queryQueue = queryQueue;
	
		// Initialize the reference to the scheduler
		this.scheduler = scheduler;

		// Create an optimizer for this thread
		queryOptimizer = new QueryOptimizer(dataManager);

                this.xqpp = new XMLQueryPlanParser();
                this.optimizer = new Optimizer();
                
		// Create a new java thread for running an instance of this object
		thread = new Thread (this,"QueryThread");

		// Call the query thread run method
		thread.start();	
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
	    } catch(ShutdownException se) {
		// should pass this back to client, for now, just print
		System.err.println("ERROR: " + se.getMessage());
	    } 
	} while (true);
    }


    /**
     *  This function executes a query
     *  
     *  @param queryInfo the QueryInfo object that contains information about
     *                   the query to be run
     */

    private void execute (QueryInfo queryInfo) throws ShutdownException {
	// Get the string version of the query
	String queryString = queryInfo.getQueryString();
	
	// Create a scanner and query parser on the fly
	// TODO: THIS HAS TO CHANGE TO REUSE SCANNER AND PARSER
	Scanner scanner;
	
	scanner = new Scanner(new EscapedUnicodeReader(
			      new StringReader(queryString)));

	queryParser = new QueryParser(scanner);
	
	// Get the parse tree
	java_cup.runtime.Symbol parseTree;
	
	try {
	    parseTree = queryParser.parse();
	} catch (Exception e) { // this is what cup throws
	    queryInfo.killQueryWithoutOperators();
	    throw new ShutdownException("Error parsing query " + e.getMessage());
	}
	
	// Get the query representation from the parse tree
	//
	query queryRep = (query) parseTree.value;
	
	// Get the logical plan from the query representation
	// THIS HAS TO CHANGE TO REUSE LOGICAL PLAN GENERATOR
	//
	logicalPlanGenerator = new LogPlanGenerator(queryRep);
	
	// Get the logical plan
	//
	LogNode logicalPlan = logicalPlanGenerator.getLogPlan();
	
	// Perform optimization on the logical plan and get optimized plan
	LogNode optimizedPlan = null;
	try {
	    optimizedPlan = queryOptimizer.optimize(logicalPlan);
	}
	catch (NoDataSourceException e) {
	    System.out.println("No Valid URLs returned");
	    queryInfo.killQueryWithoutOperators();
	    return;
	}
	
	
	// If there was an error, then exit
	if (optimizedPlan == null) {
	    System.err.println("Error in Optimizing Query");
	    queryInfo.killQueryWithoutOperators();
	    return;
	}
	
	// Send the optimized plan to scheduler for execution
	scheduler.executeOperators(optimizedPlan, queryInfo);
    }
}


