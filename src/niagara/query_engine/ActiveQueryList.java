
/**********************************************************************
  $Id: ActiveQueryList.java,v 1.5 2000/06/01 06:10:32 tufte Exp $


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

import java.util.Hashtable;
import java.util.Enumeration;

/**
 *  The ActiveQueryList class is used to store the queries that are active in the 
 *  query engine.  Objects of type <code>QueryInfo</code> are stored in this list.
 *  The public interface include the ability to add a query, remove a query and
 *  retrieve a query. Methods are synchronized because multiple threads may
 *  access the list.
 *  
 * 
 *  @see QueryInfo
 */

public class ActiveQueryList {

    ///////////////////////////////////////////////////////////////////
    // Exceptions thrown by the class                                //
    ///////////////////////////////////////////////////////////////////

    /**
     * This exception is thrown if an attempt is made to add a query
     * with an existing id
     */

    public class QueryIdAlreadyPresentException extends Exception {

    }

    
    /**
     * This exception is thrown if an attempt is made to remove a query
     * that does not exist
     */

    public class NoSuchQueryException extends Exception {

    }


    ///////////////////////////////////////////////////////////////////
    // These are the private members of the class                    //
    ///////////////////////////////////////////////////////////////////

    // A hashtable to store the active queries
    //
    private Hashtable queryList;


    ///////////////////////////////////////////////////////////////////
    // These are the methods of the class                            //
    ///////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the active query list
     */

    public ActiveQueryList(){

	// Allocate a hashtable, may want to specify init size in future.
	//
	queryList = new Hashtable();
    }

 
    /**
     * Adds a query info object to the active query list. The active query
     * list should never have a query with the same id.
     *
     * @param queryId the id of the query to be added
     * @param queryInfo the query information to add to the active list
     *
     * @exception QueryIdAlreadyPresentException If a query with the same
     *            id is already present in the list
     */

    synchronized public void addQueryInfo (int queryId, QueryInfo queryInfo)
	throws QueryIdAlreadyPresentException {
	
	// Get the integer version of query id
	//
	Integer intQueryId = new Integer(queryId);

	// First check whether there is already a query info object with
	// same id
	//
	if (queryList.get(intQueryId) != null) {

	    throw new QueryIdAlreadyPresentException();
	}

	// Add the query to the active query list
	//
	queryList.put(intQueryId, queryInfo);
    }


    /**
     * Retrieves a query info object, given the query id
     *
     * @param queryId The id of the query whose query info is to be retrieved
     *
     * @return The desired queryInfo object if it exists; null otherwise
     */

    synchronized public QueryInfo getQueryInfo (int queryId) {

	return (QueryInfo) queryList.get(new Integer(queryId));
    }


    /**
     * Removes a query info object, given the query id
     *
     * @param queryId The query id of query to be removed from active list
     *
     * @exception NoSuchQueryException No query with the given id exists
     *            in the list
     */

    synchronized public void removeQueryInfo (int queryId)
	throws NoSuchQueryException {

	if (queryList.remove(new Integer(queryId)) == null) {

	    throw new NoSuchQueryException();
	}
	System.out.println("Query removed from an active query list");
    }

    public Enumeration elements() {
	return queryList.elements();
    }

    /**
     * Returns the string representation of list
     *
     * @return string representation of list
     */
    
    synchronized public String toString () {

	return queryList.toString();
    }
}
