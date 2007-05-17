
/**********************************************************************
  $Id: QueryExecutionIF.java,v 1.8 2007/05/17 21:13:22 tufte Exp $


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


package niagara.client;

/**
 * This interface defines the methods that are called in response to 
 * events in the Gui
 *
 */

public interface QueryExecutionIF
{
    /**
	 * Execute a query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeQuery(Query query, int n) throws ClientException;
	
	/**
	 * Kill the query
	 * @param id the query id to kill
	 */
	public void killQuery(int id) throws ClientException;

	/**
	 * Request partial result from a query
	 * @param id the query id
	 */
	public void requestPartial(int id);

	/**
	 * get the type of the of the query (QueryType object)
	 * @param id the query id
	 * @return an itn @see QueryType or -1 if the type is bad
	 */
	public int getQueryType(int id);

	/**
	 * Request partial result from a query
	 * @param id the query id
	 */
	public void getNext(int id, int resultCount) throws ClientException;

	/**
	 * Checks to see if the query has received final results
	 * @param id the query id
	 * @return true if the result is final
	 */
	public boolean isResultFinal(int id);

	/**
	 * End the session with the server
	 */
	public void endSession();
	
	/**
	 * Get the query string
	 * @param id the id of the query
	 * @return the query string
	 */
	public String getQueryString(int id);
}
