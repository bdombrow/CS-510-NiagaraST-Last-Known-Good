
/**********************************************************************
  $Id: QueryRegistry.java,v 1.2 2000/07/09 05:39:46 vpapad Exp $


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

import java.util.*;
import javax.swing.tree.*;

/**
 * This class holds the result trees for all the active queries 
 *  the active queries in the client. 
 */

class QueryRegistry
{
	// private variables
	/**
	 * The map: LocalId's -> Reusult Trees and Control information
	 */
	private Map queryMap;

	/**
	 * This vector stores the current dtd list (Vector of Strings)
	 */
	private Vector dtdList;
	private Object dtdListMon = new Object();

	// Inner Classes
	/**
	 * Objects of this class are the entries of Map
	 */
	public class Entry
		{
			/**
			 * the root of the tree to be displayed
			 */
			public DefaultMutableTreeNode 
			resultTree = new DefaultMutableTreeNode(
				"Results",true);

			/**
			 * the result in text
			 */
			public StringBuffer resultText = new StringBuffer();

			/**
			 * Original Query String
			 */
			public String queryString;

			/**
			 * This is the id on the server side and is 
			 * used on the server side to route control msgs.
			 * It is -1 initially. A value <0 denotes a dont care id
			 */
			public int serverId = -1;

			/**
			 * The type of the query (XMLQL,SEQL,Trigger) @see QueryType object
			 */
			public int type = 0;
			
			/**
			 * This indicates whether no more results are expected
			 */
			public boolean isFinal = false;

			/**
			 * This indicates whether the query was killed
			 */
			public boolean isKilled = false;

			/**
			 * Indicates whether the trigger was already once fired
			 */
			public boolean alreadyFired = false;

			/**
			 * Indicate whether the trigger is paused (no erase)
			 */
			public boolean triggerPaused = false;
			
			/**
			 * Ctor
			 * @param s the query string
			 */
			public Entry(String s, int queryType)
			{
				queryString = s;
				type = queryType;
			}
			
			public String toString()
			{
			    resultTree.setUserObject("isFinal = " + isFinal
					+ " server ID = " + serverId);
				new JTreeShowThread(resultTree);
				return ".";
			}
		}
	
	/**
	 * Constructor
	 */
	public QueryRegistry()
		{
			queryMap = new HashMap();
		}

	// PUBLIC INTERFACE
	/**
	 * Register a query byt giving an ID and an object to notify
	 * @param id the id of the query which will be used for the 
	 *           map
	 * @param qString query string
	 * @return a reference to a QueryRegistry.Entry object
	 */
	public synchronized QueryRegistry.Entry registerQuery(int id, String qString, int queryType)
		{
			Integer rid = new Integer(id);
			
			// Check for ID uniqueness
			if(queryMap.containsKey(rid)){
				System.err.println("This queryID was already registered. Recode");
			}

			Entry e = new Entry(qString, queryType); 

			// put the 
			queryMap.put(rid, e);
			
			return e;
		}
	
	/**
	 * Add results to a query using the localID. Provide a list of
	 * DefautlMutableTreeNode object constructed in the response handler.
	 * @param id the id of the query to which to add results
	 * @param sid the server id as sent by the server
	 * @param results an Enumeration of DefaultMutableTreeNodes
	 * @param isFinal indicate whether this is the final result
	 */
	public synchronized void addResults(int id, int sid, List results)
		{
			Integer rid = new Integer(id);
            // Check for ID existence
			if(!queryMap.containsKey(rid)){
				System.err.println("No such query was ever executed. Recode.");
			}

			Entry e = (Entry)(queryMap.get(rid));
			
			
			if(e.serverId < 0){
				e.serverId = sid;
			} else if(sid != e.serverId){
				System.err.println("ID mismatch: Server sent " + sid + " Registry has " + e.serverId);
			}
			
			DefaultMutableTreeNode node = 
				(DefaultMutableTreeNode)(e.resultTree);
			Iterator it = results.iterator();
			while(it.hasNext()){
				// add direct leaves to the Entry tree node
				node.add((MutableTreeNode)(it.next()));
			}
			// clear the node list to avoid accidental removal of nodes
			results.clear();
		}

	/**
	 * Finalize a result
	 * @param id query id
	 */
	public synchronized void markFinal(int id)
		{
			Integer rid = new Integer(id);
            // Check for ID existence
			if(!queryMap.containsKey(rid)){
				System.err.println("No such query was ever executed. Recode.");
			}
			Entry e = (Entry)(queryMap.get(rid));
			
			e.isFinal = true;
		}
	
	/**
	 * Set the server ID of a query
	 * @param id the query id.
	 * @param serverId the id of the server.
	 */
	public synchronized void setServerId(int id, int serverId)
		{
			Integer rid = new Integer(id);
            // Check for ID existence
			if(!queryMap.containsKey(rid)){
				System.err.println("No such query was ever executed. Recode.");
			}
			Entry e = (Entry)(queryMap.get(rid));
			
			e.serverId = serverId;
		}

	/**
	 * Get the a reference to the result tree
	 * @param id the query id.
	 * @param node a DefaultMutableTreeNode object that gets the result tree
	 * @param qString the query string for display purposes
	 */
	public synchronized QueryRegistry.Entry getQueryInfo(int id)
		{
			Integer rid = new Integer(id);
			Entry e = (Entry)(queryMap.get(rid));
			
			return e;
		}

	/**
	 * Get the query type of the given id
	 * @param id the id
	 * @return the corresponding querytype
	 */
	public synchronized int getQueryType(int id)
		{
			return getQueryInfo(id).type;
		}

	/**
	 * Set the fired status of a trigger
	 * @param id the trigger id
	 */
	public void setTriggerFired(int id)
		{
			QueryRegistry.Entry e = getQueryInfo(id);
			e.alreadyFired = true;
		}

	/**
	 * returns true if the trigger was fired already
	 * @param id the id
	 * @return true if the trigger was fired
	 */
	public boolean wasAlreadyFired(int id)
		{
			return getQueryInfo(id).alreadyFired;
		}

	public void pauseTrigger(int id)
		{
			getQueryInfo(id).triggerPaused = true;
		}
	

	public void resumeTrigger(int id)
		{
			getQueryInfo(id).triggerPaused = false;
		}
	
	public boolean isTriggerPaused(int id)
		{
			return getQueryInfo(id).triggerPaused;
		}

	/**
	 * This clears the tree for the new trigger results
	 * @param id the trigger id
	 */
	public void clearTree(int id)
		{
			getQueryInfo(id).resultTree = 
				new DefaultMutableTreeNode("Results",true);
		}

	/**
	 * Check to see if the query was killed so that you don't notify
	 * the client needlessly
	 * @param id the query id
	 * @return true if the query was killed
	 */
	public synchronized boolean isKilled(int id)
		{
			Integer rid = new Integer(id);
			Entry e = (Entry)(queryMap.get(rid));
			return e.isKilled;
		}

	/**
	 * This constructs the dtd list given a string of 
	 * newline separated urls
	 * @param dtds a newline separated list of dtd's
	 */
	public void constructDTDList(String dtds)
		{
			StringTokenizer st = new StringTokenizer(dtds);
			
			synchronized(dtdListMon){
				dtdList = new Vector();
				while(st.hasMoreElements()){
					dtdList.add(st.nextToken());
				}
			
				// Notify waiter
				dtdListMon.notify();
				
				System.err.println(dtdList);
			}
		}
	
	/**
	 * This returns a vector of url strings for dtd's
	 * @return a vector of dtd strings
	 */
	public Vector getDTDList()
		{
			if(dtdList == null){
				synchronized(dtdListMon){
					while(dtdList == null){
						try{
							dtdListMon.wait();
						}
						catch(InterruptedException e){
						}
					}
				}
			}
			
			return dtdList;
		}
	
	

	
	/**
	 * This method removes a query from the query list. The removal happens 
	 * on request by the gui
	 * @param id the query id
	 */
	public synchronized void removeQuery(int id)
		{
			Integer rid = new Integer(id);
			queryMap.remove(rid);
		}

	// Debug
	public String toString()
		{
			return queryMap.toString();
		}
}
