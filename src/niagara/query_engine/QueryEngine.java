
/**********************************************************************
  $Id: QueryEngine.java,v 1.3 2001/07/17 07:03:47 vpapad Exp $


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
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;

import niagara.connection_server.NiagraServer;

/**
 *  The QueryEngine class executes queries written in XML-QL and returns
 *  an XML result.  It has the public function <code>executeQuery(string)</code>
 *  for executing queries.  This function returns an object of type QueryResult
 *  to the caller, which is used for subsequent interactions between the caller 
 *  and the query.  A QueryThread object runs each query (each of which uses 
 *  multiple PhysicalOperatorThreads)
 *
 *  @version 1.0
 *
 *
 *  @see QueryResult
 *  @see QueryThread
 *  @see PhysicalOperatorThread
 */


public class QueryEngine 
{

    ////////////////////////////////////////////////////////////////////////
    // These are the private variables of the class                       //
    ////////////////////////////////////////////////////////////////////////

    // The data manager associated with the query engine
    //
    private DataManager dataManager;

    // The queue of queries to be run. Query Threads block on get()
    // 
    private QueryQueue queryQueue;

    // The queue of operators
    //
    private PhysicalOperatorQueue opQueue;

    // A hashtable of active queries
    // 
    private ActiveQueryList activeQueries;

    // Group of threads waiting for queries to enter the system
    //
    private Vector queryThreads;

    // Group of operator threads waiting to process operators
    //
    private Vector opThreads;

    // The execution scheduler that schedules operators
    //
    private ExecutionScheduler scheduler; 

    private TrigExecutionScheduler trigScheduler;

    // A query id counter, accessed by a synchronized method 
    //
    private int lastQueryId;
    

    private NiagraServer server;
    
    /**
     * Constructor() - Create the query engine.  Initialize all thread queues
     *                 and other query engine data members
     *
     * @param server the niagara server we're running in
     * @param maxOperators the maximum number of operator threads
     * @param maxQueries the maximum number of query threads
     * @param ypServerHostName the name of the host on which the yp server runs
     * @param useConnectionManager flag to allow connection/no connection mngr
     */    
    public QueryEngine(NiagraServer server, int maxQueries, 
					   int maxOperators, 
					   String ypServerHostName,
					   int ypPort,
					   boolean useConnectionManager,
					   boolean useSearchEngine){

		System.out.println("-----------------------------------------");
		System.out.println("Starting Query Engine with these parameters");
		System.out.println("-----------------------------------------");
		System.out.println("\t-SE host = "+ypServerHostName);
		System.out.println("\t-SE port = "+ypPort);
		System.out.println("\t-maxQueryThread = "+maxQueries);
		System.out.println("\t-maxOpThreads = "+maxOperators);
		System.out.println("\t-Create connection manager = "+
						   useConnectionManager);
		System.out.println("\t-Use Permanent Connection to Search Engine = "+
						   useSearchEngine);
		System.out.println("-----------------------------------------\n");

                this.server = server;

		// Initialize the data manager
		//
		dataManager = new DataManager(".",                  // Path for temp files
									  10000,                // Disk space
									  0,                    //
									  ypServerHostName,     // YP Host
									  ypPort,               // Default YP Port
									  10,                   // Fetch threads
									  5,                    // URL Threads
									  useSearchEngine);     // Use search engine (else YP)

	
		// Initialize the query id
		//
		lastQueryId = 0;

		// Create a vector for operators scheduled for execution
		//
		opQueue = new PhysicalOperatorQueue(maxOperators);

		// Create operator thread vector and fill it with operator threads
		//
		opThreads = new Vector(maxOperators);
	
		for(int opthread = 0; opthread < maxOperators; ++opthread) {
			opThreads.addElement(new PhysicalOperatorThread(opQueue));
		}

		// Create the query scheduler
		//
		scheduler = new ExecutionScheduler(server, dataManager, opQueue);

		// Create the trig query scheduler
		//
		trigScheduler = new TrigExecutionScheduler(dataManager, opQueue);
	
		// Create the active query list
		//
		activeQueries = new ActiveQueryList();	

		// Create the query queue
		//
		queryQueue = new QueryQueue(maxQueries);

		// Create query thread vector and fill it with query threads
		//
		queryThreads = new Vector(maxQueries);

		for(int qthread = 0; qthread < maxQueries; ++qthread) {
			queryThreads.addElement(new QueryThread(dataManager,
								queryQueue,
								scheduler));
		}

		// Inform that Query Engine is ready for processing queries
		//
		System.out.println("Query Engine Ready");
    }
    

    /**
     *  The function called by all clients of the query engine who want to 
     *  run a query.  A new query information object is created, entered into
     *  the query queue and active query list, and wrapped in a query result
     *  object which is returned to the client 
     * 
     *  @param query the query to execute
     *
     *  @return qid a query id or a negative err code 
     */
    public synchronized QueryResult executeQuery(String query){
	
		// Get the next qid
		//
		int qid = CUtil.getNextQueryId();

		// Generate the output stream
		//
		Stream resultStream = new Stream(10);
	
		// Create a query information object
		//
		QueryInfo queryInfo;

		try {
			queryInfo = new QueryInfo(query,
						  qid,
						  resultStream,
						  activeQueries,
						  true);
		}
		catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
			System.err.println("Error in Assigning Unique Query Ids!");
			return null;
		}
	
		// Add it to the query queue  FIX:: May have to make this non-blocking
		/* Note, this puts the query info in a queue for a QueryThread
		 * to pick up.  The Query Thread takes care of the parsing
		 * and scheduling of this query
		 */
		queryQueue.addQuery(queryInfo);

		// Create the query result object to return to the caller
		//
		QueryResult queryResult = new QueryResult(qid, resultStream);

		// Return the query result object to the invoker of the method
		//
		return queryResult;
    }

    /**
     *  The function called by trigger manager  to 
     *  run a trigger query.  A new query information object is created, entered into
     *  the query queue and active query list, and wrapped in a query result
     *  object which is returned to the trigger manager 
     *  This function takes a fixed output stream as the output stream of the
     *  query  result.
     *  This interface is used to run a already optimized plan
     *  @param optimized logicalPlan root
     *  @param Stream resultStream
     *  @return QueryResult
     */
    /*
    public synchronized QueryResult executeOptimizedQuery(logNode planRoot, Stream resultStream){

	    //
	    // Get the next qid
	    //		
	    int qid = CUtil.getNextQueryId();
		
	    
	    // Create a query information object
	    //
	    QueryInfo queryInfo;

	    try {
	    	queryInfo = new QueryInfo("",
					  qid,
					  resultStream,
					  activeQueries,
					  true);		
	    }
	    catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
	    	System.err.println("Error in Assigning Unique Query Ids!");
	    	return null;
	    }
	    
	    // Create the query result object to return to the caller
	    //
	    QueryResult queryResult = new QueryResult(qid, resultStream);
	

	    //
	    //call Execution Scheduler to generate the physical
	    //plan and execute the group plan.
	    scheduler.executeOperators(planRoot,queryInfo);
	    
	    return queryResult;
    }
    */
    /**
     *  The function called by trigger manager  to 
     *  run a trigger query.  A new query information object is created, entered into
     *  the query queue and active query list, and wrapped in a query result
     *  object which is returned to the trigger manager 
     * 
     *  This interface is used to run a already optimized plan
     *  @param optimized logicalPlan root
     *
     *  @return QueryResult
     */

    public synchronized QueryResult executeOptimizedQuery(logNode planRoot){
        System.out.println("XXX executeOptimizedQuery called");
        
	//
	// Get the next qid
	//		
	int qid = CUtil.getNextQueryId();
	
	// Generate the output streams for each query root
	//
	Stream resultStream = new Stream(10);
	    
	// Create a query information object
	//
	QueryInfo queryInfo;
	
	try {
	    queryInfo = new QueryInfo("",
				      qid,
				      resultStream,
				      activeQueries,
				      true);		
	}
	catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
	    System.err.println("Error in Assigning Unique Query Ids!");
	    return null;
	}
	
	// Create the query result object to return to the caller
	//
	QueryResult queryResult = new QueryResult(qid, resultStream);
	
	
	//
	//call Execution Scheduler to generate the physical
	//plan and execute the group plan.
	scheduler.executeOperators(planRoot,queryInfo);
	
	return queryResult;
    }

    /**
     *  The function called by trigger manager  to 
     *  run a trigger query.  A new query information object is created, entered into
     *  the query queue and active query list, and wrapped in a query result
     *  object which is returned to the trigger manager 
     * 
     *  @param optimized logicalPlan roots
     *
     *  @return vector of QueryResults
     */

    public synchronized Vector executeGroupQuery(Vector queryRoots){
	
		//this is the vector of query results
		Vector queryResults = new Vector();
		Vector queryInfos = new Vector();	

		for (int i=0; i<queryRoots.size(); i++) {

		    //
		    // Get the next qid
		    //		
		    int qid = CUtil.getNextQueryId();
		    
		    // Generate the output streams for each query root
		    //
		    Stream resultStream = new Stream(10);
		    
		    // Create a query information object
		    //
		    QueryInfo queryInfo;
		    
		    try {
			queryInfo = new QueryInfo("",
						  qid,
						  resultStream,
						  activeQueries,
						  true);
			queryInfos.addElement(queryInfo);
			
		    }
		    catch (ActiveQueryList.QueryIdAlreadyPresentException e) {
			System.err.println("Error in Assigning Unique Query Ids!");
			return null;
		    }
		    
		    // Create the query result object to return to the caller
		    //
		    QueryResult queryResult = new QueryResult(qid, resultStream);
		    queryResults.addElement(queryResult);
		}
		
		//
		//call Trigger Execution Scheduler to generate the physical
		//plan and execute the group plan.
		trigScheduler.executeOperators(queryRoots,queryInfos);
		
		// Return the query results object to the invoker of the method
		//
		// System.err.println("Now cleanning up");
		trigScheduler.cleanUpAlloc();
		return queryResults;
    }


    /**
     *  Get the DTD list from DM (which contacts ther YP if list is not cached)
     * 
     */
    public Vector getDTDList()
		{
			Vector ret = null;
			try{

			ret = dataManager.getDTDList();

			}catch(Exception dmce){
				System.out.println("The data manager has been previously shutdown, re-start the system");
				return null;
			}
			return ret;
		}


    /**
     *  Enable Caching in the data manager
     * 
     */
    public void enableDataManagerCache()
		{
			dataManager.enableCache();
		}


    /**
     *  Enable Caching in the data manager
     * 
     */
    public void disableDataManagerCache()
		{
			dataManager.disableCache();
		}


    /**
     *  Dumps the query engine and its components as a string for debugging 
     *
     *  @return a string representation of the query engine
     */

    public synchronized String toString() {

		String retStr = new String("\n**********************************************************************\n");
		retStr += "**    Q u e r y      E n g i n e      D u m p                       **\n";
		retStr += "**********************************************************************\n";
		retStr += "lastQueryId: "+lastQueryId+"\n";
		retStr += "+++++++++++++++ Waiting Queries ++++++++++++++++++++++++++++++++++++++\n";
		retStr += queryQueue.toString()+"\n\n";
		retStr += "+++++++++++++++ Active Queries +++++++++++++++++++++++++++++++++++++++\n";
		retStr += activeQueries.toString()+"\n";
		retStr += "+++++++++++++++ Threads ++++++++++++++++++++++++++++++++++++++++++++++\n\n";
		retStr += queryThreads.toString()+"\n\n";
		retStr += opThreads.toString()+"\n\n";
		retStr += "+++++++++++++++ Operators ++++++++++++++++++++++++++++++++++++++++++++\n";
		retStr += opQueue.toString()+"\n";
		retStr += "+++++++++++++++ Scheduler ++++++++++++++++++++++++++++++++++++++++++++\n";
		retStr += scheduler.toString()+"\n";
		retStr += "**********************************************************************\n";

		return retStr;
    }

    /**
     *  Gracefully shutdown the query engine.
     *
     */
    public void shutdown()
		{
		    dataManager.shutdown();
		}

    /**
     *  return the dataManager instance in the query engine.
     */
    public DataManager getDataManager()
    {
	return dataManager;
    }
    
    public void  enqueueQuery(QueryInfo queryInfo) {
	queryQueue.addQuery(queryInfo);
    }

    public ExecutionScheduler getScheduler() {
        return scheduler;
    }
}



