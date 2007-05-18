
/**********************************************************************
  $Id: ConnectionManager.java,v 1.22 2007/05/18 00:25:28 jinli Exp $


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

import gnu.regexp.*;

import niagara.utils.*;

/**
 * This class implements the QueryExecutionIF interface and 
 * coordinates the messages that are sent to the server
 * 
 */

public class ConnectionManager implements QueryExecutionIF {
    // constants
    public static final int SERVER_PORT = 9020;
   
    public static final String REQUEST_MESSAGE = "requestMessage";
    public static final String REQUEST_DATA = "requestData";
    public static final String LOCAL_ID = "localID";
    public static final String SERVER_ID = "serverID";
    public static final String REQUEST_TYPE = "requestType";
    static final String RESULT_TYPE = "resultType";
    public static final String INTERMITTENT = "intermittent";
    public static final String KILL_PREPARE = "kill-prepared";
    
    public static final String KILL_QUERY = "kill_query";
    public static final String GET_NEXT = "get_next";
    public static final String GET_DTD_LIST = "get_dtd_list";
    public static final String GET_DTD = "get_dtd";
    public static final String GET_PARTIAL = "get_partial";
    public static final String SUSPEND_QUERY = "suspend_query";
    public static final String RESUME_QUERY = "resume_query";
    public static final String RUN_GC = "gc";
    public static final String SHUTDOWN = "shutdown";
    public static final String DUMPDATA = "dumpdata";

    public static final String BEGIN_REQUEST_DATA = "<" + REQUEST_DATA + ">";
    public static final String END_REQUEST_DATA = "</" + REQUEST_DATA + ">";
    public static final String END_REQUEST_MESSAGE = "</" + REQUEST_MESSAGE + ">";
    
    // private variables
    /**
     * Query id generator
     */
    private int qid = 1;
    
    /**
     * The connection reader that parses the input
     * (Is a thread)
     */
    private AbstractConnectionReader connectionReader;
    
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

    private boolean KT_PERFORMANCE = true;

    // Constructors
    public ConnectionManager(String hostname, int port, UIDriverIF ui) {
	this(new ConnectionReader(hostname, port, ui));
    }
    
    public ConnectionManager(AbstractConnectionReader cr) {
        this.connectionReader = cr;
        
	// initialize the call back interface of the client
	this.ui = cr.getUI();
	
	// Get the output writer from the connection reader
	writer = connectionReader.getPrintWriter();
	
	// Get the registry from the connection reader
	reg = connectionReader.getQueryRegistry();

	// start up the threads and finish
	Thread crThread = new Thread(connectionReader, "ConnectionReader");
			
	crThread.start();
	
	// Send a request to get the DTD's
	if(!KT_PERFORMANCE) {
	    sendDTDRequest();
	}
    }

    // Public interface
	
    // QueryExecutionIF methods

    public AbstractConnectionReader getConnectionReader() {
	return connectionReader;
    }

	

    /**
     * The back end to the different executeQuery calls
     * @param s the query string
     * @param nResults limit of initial results (after this hitting getnext is required)
     * @param queryType the type of the query (QueryType object fields)
     * @param attr the string to be sent to the server determing the type of the execution
     * @return the id of the query in the registry
     */
    public int executeQuery(Query query, int nResults) throws ClientException {
	int id = getID(); // just does increment
	String attr = query.getCommand();
	int queryType = query.getType();

	// Register the query
	final QueryRegistry.Entry e = 
	    reg.registerQuery(id, query);
	// Set the query type
	e.type = queryType;
	    
	synchronized(writer){
	    writer.println(formatMessageHeader(id, -1, attr));
	    writer.println(BEGIN_REQUEST_DATA);

	    writer.println("<![CDATA[");
		
	    System.out.println(formatMessageHeader(id, -1, attr));
	    System.out.println(BEGIN_REQUEST_DATA);
	    System.out.println("<![CDATA[");
	    // XXX this is total hack!
	    // the query may contain CDATA sections itself (argh!)
	    // Hack as follows:
	    // Change all ]]> to ESC]ESC]ESC> 
	    // Of course this still does not cover all the cases
	    String qtext = query.getText();
	    try {
		RE re = new RE("]]>");
		String esc = re.substituteAll(qtext, "ESC]ESC]ESC>");
		writer.println(esc);
		writer.println("]]>");
		writer.println(END_REQUEST_DATA);
		writer.println(END_REQUEST_MESSAGE);
		System.out.println(esc);
		System.out.println("]]>");
		System.out.println(END_REQUEST_DATA);
		System.out.println(END_REQUEST_MESSAGE);
	    }
                catch (REException rexc) {
                    System.out.println("CDATA escaping: regular expression failure");
                    System.exit(-1);
                }
	    }
	    
        if (queryType == QueryType.QP
            || queryType == QueryType.EXPLAIN
            || queryType == QueryType.PREPARE
            || queryType == QueryType.EXECUTE_PREPARED
            || queryType == QueryType.KILL_PREPARED) {
            getNext(id, nResults);
        }

	return id;
    }
    
    /**
     * Kill the query
     * @param id the query id to kill
     */
    public void killQuery(int id) throws ClientException {
	QueryRegistry.Entry e =
	    reg.getQueryInfo(id);
	
	int sid = e.getServerId();
	
	synchronized(writer){
	    // Send the request to the server
	    writer.println(formatMessageHeader(id, sid, KILL_QUERY));
	    writer.println(END_REQUEST_MESSAGE);
	}
	// Mark this query as killed
	e.isKilled = true;
    }
    
    /**
     * Request partial
     * @param id the query id to kill
     */
    public void requestPartial(int id) {
	writeMessage(id, GET_PARTIAL);
    }
    
    public void runSpecialFunc(String func) {
	int id = getID();
	Query query = new SpecialFunctionQuery(func);
	final QueryRegistry.Entry e = 
	    reg.registerQuery(id, query);
	// Set the query type
	e.type = QueryType.NOTYPE;
	writeMessageNoSID(id, func); 
    }
    
    /**
     * Get next resultCount results
     * @param id the query id to kill
     * @param resultCount then # of result
     */
    
    public void getNext(int id, int resultCount) throws ClientException
    {
	QueryRegistry.Entry e =
	    reg.getQueryInfo(id);
	
	// Check for null e 
	if(e == null){
	    ui.errorMessage(id, "Select a query first");
	    return;
	}
	int sid = e.getServerId();
	
	synchronized(writer){
	    // Send the request to the server
	    writer.println(formatMessageHeader(id, sid, GET_NEXT));
	    writer.print(BEGIN_REQUEST_DATA);
	    writer.print(resultCount);
	    writer.println(END_REQUEST_DATA);
	    writer.println(END_REQUEST_MESSAGE);
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

    public void queryError(int id) {
	QueryRegistry.Entry e = reg.getQueryInfo(id);
	if(e != null)
	    e.errorWakeUpAnyWaiters();
	else
	    System.err.println("Invalid server id on query error - can't wake up main thread");
    }
    
    // Private functions
    /**
     * Return a session unique qid
     */
    private int getID() {
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
    private void writeMessage(int id, String msg) {
	try {
	    QueryRegistry.Entry e =	reg.getQueryInfo(id);
	    int sid = e.getServerId();
	    writeMessage(id, msg, sid);
	} catch (ClientException e) {
	    System.err.println("Bad server id when writing message " + e.getMessage());
	    throw new PEException(e.getMessage());
	}
    }
    
    /**
     * this class is used for the simple messages without request data
     */
    private void writeMessageNoSID(int id, String msg) {
	writeMessage(id, msg, -1);
    }

    private String formatMessageHeader(int localId, int serverId, String request_type) {
    	StringBuffer sb = new StringBuffer();
	sb.append("<").append(REQUEST_MESSAGE).append(" ");
	sb.append(LOCAL_ID).append(" ='").append(String.valueOf(localId)).append("' ");
	sb.append(SERVER_ID).append(" ='").append(String.valueOf(serverId)).append("' ");
	sb.append(REQUEST_TYPE).append(" ='").append(request_type).append("' ");
	/* only for test purpose - to test text output */ 
	//sb.append(RESULT_TYPE).append(" ='").append("text").append("' ");
	Query q = reg.getQueryInfo(localId).query;
	if (q.isIntermittent())
	    sb.append("intermittent='true' ");
	else
	    sb.append("intermittent='false' ");
	
    sb.append(">");
	return sb.toString();
    }

    private void writeMessage(int id, String msg, int sid) {
	synchronized(writer){
	    writer.println(formatMessageHeader(id, sid, msg));
	    writer.println(END_REQUEST_MESSAGE);
	}
    }
}


