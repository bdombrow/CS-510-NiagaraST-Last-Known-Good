
/**********************************************************************
  $Id: QueryQueue.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

import niagara.utils.*;

/**
 *  The QueryQueue class stores queries as they are entered into the QueryEngine.
 *  Multiple clients threads (producers) may produce queries via the QueryEngine
 *  method executeQuery(string), which calls the addQuery() method of the query queue.  
 *  A queue of primed threads wait (consumers) on the function getQuery() to execute 
 *  the queries.  The query queue is implemented as a SynchronizedQueue.  The 
 *  queue stores queries as QueryInfo objects.
 *
 *  @see QueryThread
 *  @see QueryInfo
 *  @see QueryEngine
 *  @see SynchronizedQueue
 *  @see Queue
 */
public class QueryQueue {
	
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //   Data members of the QueryQueue Class
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    // A synchronized queue for storing the querys
    //
    private SynchronizedQueue queryQueue;

    
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //   Methods of the QueryQueue Class
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    /**
     * This is the constructor for the QueryQueue class that initializes
     * it to an empty queue.
     *
     * @param maxCapacity The maximum capacity of the query queue
     */
    public QueryQueue (int maxCapacity) 
    {
	// Call the constructor of the super class
	//
	super();

	// Create a synchronized queue to server as an query queue
	//
	queryQueue = new SynchronizedQueue (maxCapacity);

	// End of function
	//
	return;
    }
		     

    /**
     * This function adds an query to the query queue
     *
     * @param query The query to be added to the queue
     */
    public void addQuery (QueryInfo query) 
    {
	// Add the query to the end of the queue
	//
	queryQueue.put(query, true);
	return;
    }


    /**
     * This function gets an query from the query queue
     *
     * @return The query at the head of the queue
     */
    public QueryInfo getQuery () 
    {
	// Get the query from the queue
	//
	QueryInfo tempQuery = (QueryInfo) queryQueue.get();

	// Return the query
	//
	return tempQuery;
    }

    public synchronized String toString()
    {
	return queryQueue.toString();
    }
}








