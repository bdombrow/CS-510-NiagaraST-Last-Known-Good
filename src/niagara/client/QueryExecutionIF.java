
/**********************************************************************
  $Id: QueryExecutionIF.java,v 1.2 2000/06/26 21:48:12 vpapad Exp $


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

import javax.swing.tree.*;
import java.net.*;
import java.util.*;

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
	 * @return the id of the query in the registry
	 */
	public int executeQuery(String query);

    /**
	 * Execute a query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeQuery(String query, int n);
	
	/**
	 * Execute a trigger query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeTriggerQuery(String query, int n);

        /**
	 * Execute a query, with the specified query plan
	 * @param s the query plan in XML format
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
         public int executeQPQuery(String query, int n);

	/**
	 * Execute a Search Engine  query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeSEQuery(String query, int n);
	
	/**
	 * Kill the query
	 * @param id the query id to kill
	 */
	public void killQuery(int id);

	/**
	 * Suspend the query
	 * @param id the query id to kill
	 */
	public void suspendQuery(int id);

	/**
	 * Resume the query
	 * @param id the query id to kill
	 */
	public void resumeQuery(int id);

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
	public void getNext(int id, int resultCount);

	/**
	 * Checks to see if the query has received final results
	 * @param id the query id
	 * @return true if the result is final
	 */
	public boolean isResultFinal(int id);

	/**
	 * Get result of a query given the query id
	 * @param id The id of the query
	 * @return the MutableTreeNode object to be displayed
	 */
	public DefaultMutableTreeNode getQueryResultTree(int id);

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

	/** 
	 * Get the dtd list from the server
	 */
	public Vector getDTDList();

	/**
	 * Generate a tree for search engine
	 * @param url the dtd url
	 */
	public DefaultMutableTreeNode generateSETree(URL dtdURL);
	
	/**
	 * Generate a tree for xmlql
	 * @param url the dtd url
	 */
	public DefaultMutableTreeNode generateXMLQLTree(URL dtdURL);
}
