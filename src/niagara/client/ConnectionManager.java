
/**********************************************************************
  $Id: ConnectionManager.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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

import java.io.*;
import java.net.*;
import javax.swing.tree.*;
import java.util.*; 

import niagara.client.dtdTree.DTD;

/**
 * This class implements the QueryExecutionIF interface and 
 * coordinates the messages that are sent to the server
 * 
 */

public class ConnectionManager implements QueryExecutionIF
{
	// constants
	public static final int SERVER_PORT = 9020;

	public static final String REQUEST_MESSAGE = "requestMessage";
	public static final String REQUEST_DATA = "requestData";
	public static final String LOCAL_ID = "localID";
	public static final String SERVER_ID = "serverID";
	public static final String REQUEST_TYPE = "requestType";
	
	public static final String EXECUTE_QE_QUERY = "execute_qe_query";
	public static final String EXECUTE_TRIGGER_QUERY = "execute_trigger_query";
	public static final String EXECUTE_SE_QUERY = "execute_se_query";
	public static final String KILL_QUERY = "kill_query";
	public static final String GET_NEXT = "get_next";
	public static final String GET_DTD_LIST = "get_dtd_list";
	public static final String GET_DTD = "get_dtd";
	public static final String GET_PARTIAL = "get_partial";
	public static final String SUSPEND_QUERY = "suspend_query";
	public static final String RESUME_QUERY = "resume_query";
	
	
	// private variables
	/**
	 * Query id generator
	 */
	private int qid = 1;

	/**
	 * The connection reader that parses the input
	 * (Is a thread)
	 */
	private ConnectionReader connectionReader;

	/**
	 * The dtd cache for the client
	 */
	private DTDCache dtdCache;

	/**
	 * The output writer to write control 
	 * information to the server
	 */
	private PrintWriter writer;
	
	/**
	 * A reference to the registry inorder to implement the interface
	 */
	private QueryRegistry reg;
	
	/**
	 * The thread that initiates changes to the UI
	 */
	private UIDriverIF ui;

	// Constructor
	public ConnectionManager(String hostname, int port, UIDriverIF ui)
		{
			// create a cache for the dtd's
			dtdCache = new DTDCache();

			// Create a ConnectionReader object
			connectionReader = new ConnectionReader(hostname, port, ui, dtdCache);

			// initialize the call back interface of the client
			this.ui = ui;
			
			// Get the output writer from the connection reader
			writer = connectionReader.getPrintWriter();

			// Get the registry from the connection reader
			reg = connectionReader.getQueryRegistry();

			// start up the threads and finish
			Thread crThread = new Thread(connectionReader);
			
			crThread.start();
			
			// Send a request to get the DTD's
			sendDTDRequest();
		}

	// Public interface
	
	// QueryExecutionIF methods

	/**
	 * Get the dtd list
	 */
	public Vector getDTDList()
		{
			Vector dtds = reg.getDTDList();
			
//  			try{
//  				URL url = new URL((String)dtds.elementAt(0));
//  				DefaultMutableTreeNode n = generateXMLQLTree(url);
//  				if(n != null) new JTreeShowThread(n);
				
//  				url = new URL((String)dtds.elementAt(1));
//  				n = generateXMLQLTree(url);
//  				if(n != null) new JTreeShowThread(n);

//  				url = new URL((String)dtds.elementAt(0));
//  				n = generateXMLQLTree(url);
//  				if(n != null) new JTreeShowThread(n);
//  			}
//  			catch(Exception e){
//  				e.printStackTrace();
//  			}			

			return dtds;
		}

	/**
	 * Generate a tree for search engine
	 * @param url the dtd url
	 */
	public DefaultMutableTreeNode generateSETree(URL dtdURL)
		{
			String s = dtdCache.getDTD(dtdURL.toString());
			if(s != null){
				return DTD.generateSETree(s);
			} else {
				return DTD.generateSETree(
					getDTDFromServer(dtdURL.toString()));
			}
		}

	/**
	 * Generate a tree for xmlql
	 * @param url the dtd url
	 */
	public DefaultMutableTreeNode generateXMLQLTree(URL dtdURL)
		{
			String s = dtdCache.getDTD(dtdURL.toString());
			if(s != null){
				return DTD.generateXMLQLTree(s);
			} else {
				return DTD.generateXMLQLTree(
					getDTDFromServer(dtdURL.toString()));
			}
		}

	/**
	 * Send a request message to get the 
	 * text of a dtd. It returns when the dtd
	 * from the server has arrived in the 
	 * cache
	 * @param dtdURL the URL of the dtd
	 */
	
	String getDTDFromServer(String dtdURL)
		{
			// get the next id for the dtd request
			final int id = dtdCache.registerDTD(dtdURL);
			
			// write the request to the server
			synchronized(writer){
				// Send the query to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"" + id +"\" "+ SERVER_ID +"=\"-1\""
							   + " " + REQUEST_TYPE +"= \"" + GET_DTD + "\">");
				writer.print("<" + REQUEST_DATA + ">");
				writer.print(dtdURL.toString());
				writer.println("</" + REQUEST_DATA + ">");
				writer.println("</" + REQUEST_MESSAGE + ">");
			}

			synchronized(this){
				while(!(dtdCache.hasDTDArrived(id))){
					try{
						wait(500);
					}
					catch(InterruptedException ee){
						ee.printStackTrace();
					}
				}
			}
						
			String ret = (String) dtdCache.getDTD(dtdURL);
			return ret;
		}
	

	/**
	 * Execute a query (default: 10 first results)
	 * @param s the query string
	 * @return the id of the query in the registry
	 */
	public int executeQuery(String query)
		{
			return this.executeQuery(query, 10, QueryType.XMLQL, EXECUTE_QE_QUERY);
		}

    /**
	 * Execute a query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeQuery(String query, int n)
		{
			return this.executeQuery(query, n, QueryType.XMLQL, EXECUTE_QE_QUERY);
		}
	
	/**
	 * Execute a trigger query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeTriggerQuery(String query, int n)
		{
			return this.executeQuery(query, n, QueryType.TRIG, EXECUTE_TRIGGER_QUERY);
		}

	/**
	 * Execute a Search Engine  query
	 * @param s the query string
	 * @param n limit of initial results (after this hitting getnext is required)
	 * @return the id of the query in the registry
	 */
	public int executeSEQuery(String query, int n)
		{
			return this.executeQuery(query, n, QueryType.SEQL, EXECUTE_SE_QUERY);
		}

	/**
	 * The back end to the different executeQuery calls
	 * @param s the query string
	 * @param nResults limit of initial results (after this hitting getnext is required)
	 * @param queryType the type of the query (QueryType object fields)
	 * @param attr the string to be sent to the server determing the type of the execution
	 * @return the id of the query in the registry
	 */
	private int executeQuery(String query, int nResults, int queryType, String attr)
		{
			int id = getID();

			System.err.println("Executing " + attr);
			
			// Register the query
			final QueryRegistry.Entry e = 
				reg.registerQuery(id, query, queryType);
			// Set the query type
			e.type = queryType;
			
			synchronized(writer){
				// Send the query to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"" + id +"\" "+ SERVER_ID +"=\"-1\""
							   + " " + REQUEST_TYPE +"= \"" + attr + "\">");
				writer.println("<" + REQUEST_DATA + ">");
				writer.println("<![CDATA[");
				// the actual query
				writer.println(query);
				writer.println("]]>");
				writer.println("</" + REQUEST_DATA + ">");
				writer.println("</" + REQUEST_MESSAGE + ">");
			}
			
			// TODO wait for server qid to come accross.

			//this.getNext(id, nResults);
			if(queryType == QueryType.XMLQL){
				final ConnectionManager cm = this;
				final int nRes = nResults;
				final int qid = id;
				Thread t = new Thread(){
					public void run(){
						synchronized(this){
							while(e.serverId == -1){
								try{
									this.wait(500);
								}
								catch(InterruptedException ee){
									ee.printStackTrace();
								}
							}
							cm.getNext(qid, nRes);
						}
					}
				};
				// Start up the thread to send the getNext.
				t.start();
			}
			
			
			return id;
		}

	/**
	 * Kill the query
	 * @param id the query id to kill
	 */
	public void killQuery(int id)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			int sid = e.serverId;
			if(e.type == QueryType.SEQL){
				// Kill action has nothing to do.
				return;
			}
			
			synchronized(writer){
				// Send the request to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"" + id +"\" "
							   + SERVER_ID +"=\"" + sid +"\""
							   + " " + REQUEST_TYPE +"= \"" + KILL_QUERY + "\">");
				writer.println("</" + REQUEST_MESSAGE + ">");
			}
            // Mark this query as killed
			e.isKilled = true;
		}

	/**
	 * Suspend the query
	 * @param id the query id to kill
	 */
	public void suspendQuery(int id)
		{
			int queryType = reg.getQueryType(id);
			if(queryType == QueryType.XMLQL){
				writeMessage(id, SUSPEND_QUERY);
			} else if(queryType == QueryType.TRIG){
				reg.pauseTrigger(id);
			} else {}//se stuff
		}
	

	/**
	 * Resume the query
	 * @param id the query id to kill
	 */
	public void resumeQuery(int id)
		{
			int queryType = reg.getQueryType(id);
			if(queryType == QueryType.XMLQL){
				writeMessage(id, RESUME_QUERY);
			} else if(queryType == QueryType.TRIG){
				reg.resumeTrigger(id);
			} else {}//se stuff
		}
	

	/**
	 * Request partial
	 * @param id the query id to kill
	 */
	
	public void requestPartial(int id)
		{
			writeMessage(id, GET_PARTIAL);
		}

	/**
	 * Get next resultCount results
	 * @param id the query id to kill
	 * @param resultCount then # of result
	 */
	
	public void getNext(int id, int resultCount)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			// Check for null e 
			if(e == null){
				ui.errorMessage("Select a query first");
				return;
			}
			int sid = e.serverId;
			
			synchronized(writer){
				// Send the request to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"" + id +"\" "
							   + SERVER_ID +"=\"" + sid +"\""
							   + " " + REQUEST_TYPE +"= \"" + GET_NEXT + "\">");
				writer.print("<" + REQUEST_DATA + ">");
				writer.print(resultCount);
				writer.println("</" + REQUEST_DATA + ">");
				writer.println("</" + REQUEST_MESSAGE + ">");
							   
			}
		}
	/**
	 * Get result of a query given the query id. DO NOT mess 
	 * with this node just display it
	 * @param id The id of the query
	 * @return the MutableTreeNode object to be displayed
	 */
	public DefaultMutableTreeNode getQueryResultTree(int id)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			if(e != null){
				return e.resultTree;
			} else {
				return new DefaultMutableTreeNode("NULL in registry. RECODE!!");
			}
		}
		
	/**
	 * Get the query string
	 * @param id the id of the query
	 * @return the query string
	 */
	public String getQueryString(int id)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			if(e != null){
				return e.queryString;
			} else {
				return "NULL in registry. RECODE!!";
			}
		}

	/**
	 * get the type of the of the query (QueryType object)
	 * @param id the query id
	 */
	public int getQueryType(int id)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			if(e != null){
				return e.type;
			} else {
				return QueryType.NOTYPE;
			}
		}

	/**
	 * Checks to see if the query has received final results
	 * @param id the query id
	 * @return true if the result is final
	 */
	public boolean isResultFinal(int id)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);
			
			if(e != null){
				return e.isFinal;
			} else {
				return true;
			}
		}

	/**
	 * End the session with the server
	 */
	public void endSession()
		{
			try{
				connectionReader.endRequestSession();
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}

	// Private functions
	/**
	 * Return a session unique qid
	 */
	private int getID()
		{
			return qid++;
		}

	/**
	 * This sends a dtd request to the server
	 */
	private void sendDTDRequest()
		{
			synchronized(writer){
				// Send the request to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"-1\" "
							   + SERVER_ID +"=\"-1\""
							   + " " + REQUEST_TYPE +"= \"" + GET_DTD_LIST + "\">");
				writer.println("</" + REQUEST_MESSAGE + ">");
			}
		}

	/**
	 * this class is used for the simple messages without request data
	 */
	private void writeMessage(int id, String msg)
		{
			QueryRegistry.Entry e =
				reg.getQueryInfo(id);

			int sid = e.serverId;
			
			synchronized(writer){
				// Send the request to the server
				writer.println("<" + REQUEST_MESSAGE + " " + LOCAL_ID +"=\"" + id +"\" "
							   + SERVER_ID +"=\"" + sid +"\""
							   + " " + REQUEST_TYPE +"= \"" + msg + "\">");
				writer.println("</" + REQUEST_MESSAGE + ">");
			}
		}

	/////////////////////////////////////////////////////////////
	// DEBUG MAIN
	/////////////////////////////////////////////////////////////
	public static void main(String argv[])
		{
			
		}	
}
