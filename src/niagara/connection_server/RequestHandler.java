/**********************************************************************
  $Id: RequestHandler.java,v 1.13 2002/08/16 17:55:53 tufte Exp $


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


package niagara.connection_server;

import java.io.*;
import java.util.*;
import java.net.*;
import niagara.query_engine.*;
import niagara.trigger_engine.*;
import niagara.xmlql_parser.op_tree.logNode;
import niagara.data_manager.DataManager;
import niagara.utils.PEException;

/**There is one request handler per client and receives all the requests from that client
   Then that request is further dispatched to the appropriate module and results sent back
*/
public class RequestHandler {

    // Hashtable of queries
    QueryList queryList;
    
    // The parser which listens to the stream coming from client
    RequestParser requestParser;

    // The Writer to which all the results have to go
    BufferedWriter outputWriter;

    // The server which instantiated this RequestHandler
    NiagraServer server;
    
    // Every query is given a server query id. This is the counter for giving out that service id
    int lastQueryId = 0;

    private boolean dtd_hack; // True if we want to add HTML entities to the result    

    /**Constructor
       @param sock The socket to read from 
       @param server The server that has access to other modules
    */
    public RequestHandler(Socket sock, NiagraServer server, boolean dtd_hack)
	throws IOException{
	this.dtd_hack = dtd_hack;

	// A hashtable of queries with qid as key
	this.queryList = new QueryList();
	this.outputWriter = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
	this.server = server;
	sendBeginDocument();

	this.requestParser = new RequestParser(sock.getInputStream(),this);
	this.requestParser.startParsing();
    }


    public RequestHandler(InputStream is, OutputStream os, NiagraServer server)
        throws IOException{
	this.dtd_hack = false;
	// A hashtable of queries with qid as key
	System.out.println("inter-server request handler started...");
	this.queryList = new QueryList();
	this.outputWriter = new BufferedWriter(new OutputStreamWriter(os));
	this.server = server;
	sendBeginDocument();

	this.requestParser = new RequestParser(is, this);
	this.requestParser.startParsing();
    }



    public RequestHandler(Socket sock, NiagraServer server) 
	throws IOException {
	    this(sock, server, true);
    }

    // Send the initial string to the client
    private void sendBeginDocument() throws IOException { // DTD is hack for sigmod record
	String header =  "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n";
	if (dtd_hack) {
	    // added to support the SigmodRecord Entities
	    header = header + EntitySupplement.returnEntityDefs("CarSet.cfg");
	}
	header = header + "<response>\n";
	outputWriter.write(header);
	outputWriter.flush();
    }


    /**Handle the request that just came in from the client
       @param request The request that needs to be handled
     */
    public void handleRequest(RequestMessage request) {
	try{
	    
	    // Handle the request according to requestType
	    switch(request.getIntRequestType()){
	    //   EXECUTE_QUERY request
	    //-------------------------------------
	    case RequestMessage.EXECUTE_QP_QUERY:
		{
		    // assign a new query id to this request
		    int qid = getNextConnServerQueryId();

		    XMLQueryPlanParser xp = new XMLQueryPlanParser();
		    logNode topNode = xp.parse(request.requestData);
		
		    QueryResult qr = 
			server.qe.executeOptimizedQuery(topNode);
		    
		    request.serverID = qid;
		    
		    /* create and populate the query info
		     * We assume that if the top node is Accumulate
		     * operator, then we should run Accumulate file
		     * query
		     * Oh boy, this is ugly - I use the fact that the
		     * query has already been parsed to help set up
		     * the query info. What will I do when I have to
		     * deal with a real query?? Modify queryInfo after
		     * the fact?? Ugly!!!
		     */
		    ServerQueryInfo serverQueryInfo;
		    if(topNode.isAccumulateOp()) {
			System.out.println("top node is accumulate: " +
					   topNode.getAccumFileName());
			serverQueryInfo = new ServerQueryInfo(qid,
					ServerQueryInfo.AccumFile);
			serverQueryInfo.accumFileName = topNode.getAccumFileName();
		    } else {
			serverQueryInfo = new ServerQueryInfo(qid,
				       ServerQueryInfo.QueryEngine);
		    }
		    serverQueryInfo.queryResult = qr;
		    queryList.put(qid,serverQueryInfo);

		    // start the transmitter thread for sending results back
		    serverQueryInfo.transmitter = 
			new ResultTransmitter(this,serverQueryInfo,request);

		
		    //send the query ID out
		    sendQueryId(request);
		}
		break;

	    case RequestMessage.EXECUTE_QE_QUERY:
		
		// assign a new query id to this request
		int qid = getNextConnServerQueryId();
		
		// now give the query to the query engine
		QueryResult qr = server.qe.executeQuery(request.requestData);
		
		request.serverID = qid;

		// create and populate the query info
		ServerQueryInfo queryInfo = new ServerQueryInfo(qid,ServerQueryInfo.QueryEngine);
		queryInfo.queryResult = qr;
		queryList.put(qid,queryInfo);

		// start the transmitter thread for sending results back
		queryInfo.transmitter = 
			new ResultTransmitter(this,queryInfo,request);

		
		//send the query ID out
		sendQueryId(request);
		break;
	    

	    case RequestMessage.EXECUTE_TRIGGER_QUERY:
			
		// Get the next qid
		qid = getNextConnServerQueryId();
		
		request.serverID = qid;

		queryInfo = new ServerQueryInfo(qid,ServerQueryInfo.TriggerEngine);

		// now enqueue the query to the query engine
		queryInfo.triggerName = server.triggerManager.createTrigger(request.requestData,
									    queryInfo.queryResultQueue);
		    
		// if some error happened
		if (queryInfo.triggerName == null) {
		    ResponseMessage response = new ResponseMessage(request,ResponseMessage.ERROR);
		    response.responseData = "The trigger could not be installed";
		    sendResponse(response);
		    break;
		}

		// start the transmitter thread for sending results back		    
		queryInfo.transmitter = 
		    new ResultTransmitter(this,queryInfo,request);
				    		
		queryList.put(qid,queryInfo);
    
		System.out.println("Trigger name is "+queryInfo.triggerName);
		//send the query ID out
		sendQueryId(request);
		
		break;


	 case RequestMessage.EXECUTE_SE_QUERY:
			
		// Get the next qid
		qid = getNextConnServerQueryId();
		request.serverID = qid;
		
		// Create a query info object
		queryInfo = new ServerQueryInfo(qid,ServerQueryInfo.SearchEngine);
		queryInfo.searchEngineQuery = request.requestData;		    

		// start the transmitter thread for sending results back
		queryInfo.transmitter = 
		    new ResultTransmitter(this,queryInfo,request);
		
		//send the query ID out
		sendQueryId(request);
		
		break;
	
	    case RequestMessage.GET_DTD:
		// Get the next qid
		qid = getNextConnServerQueryId();
		request.serverID = qid;
		
		queryInfo = new ServerQueryInfo(qid,ServerQueryInfo.GetDTD);
		// start the transmitter thread for sending results back
		queryInfo.transmitter = 
		    new ResultTransmitter(this,queryInfo,request);
		break;

 	    case RequestMessage.SUSPEND_QUERY:
		// get the queryInfo of this query
 		queryInfo = queryList.get(request.serverID);
		// Respond to invalid queryID
		if(queryInfo == null) {
		    System.out.println("QID Recvd "+request.serverID);
		    throw new InvalidQueryIDException();
		}

 		if (queryInfo.transmitter != null)
 		    queryInfo.transmitter.suspend();
 		break;

 	    case RequestMessage.RESUME_QUERY:
		// get the queryInfo of this query
 		queryInfo = queryList.get(request.serverID);
		// Respond to invalid queryID
		if(queryInfo == null) {
		    System.out.println("QID Recvd "+request.serverID);
		    throw new InvalidQueryIDException();
		}

 		if (queryInfo.transmitter != null)
 		    queryInfo.transmitter.resume();
 		break;
	
		//-------------------------------------
		//   KILL_QUERY request
		//-------------------------------------
	    case RequestMessage.KILL_QUERY:		
		killQuery(request.serverID);
		break;
		
	    case RequestMessage.GET_DTD_LIST:
		sendDTDList(request);
		break;

		//-------------------------------------
		//   GET_NEXT request
		//-------------------------------------
	    case RequestMessage.GET_NEXT:
		// get the queryInfo of this query
		queryInfo = queryList.get(request.serverID);
		
		// Respond to invalid queryID
		if(queryInfo == null) {
		    System.out.println("QID Recvd "+request.serverID);
		    throw new InvalidQueryIDException();
		}

		queryInfo.transmitter.handleRequest(request);
				
		break;

		//-------------------------------------
		//   GET_PARTIAL request
		//-------------------------------------
	    case RequestMessage.GET_PARTIAL:		
		// Get the queryInfo object for this request
		queryInfo = queryList.get(request.serverID);

		// Respond to invalid queryID
		if(queryInfo == null)
		    throw new InvalidQueryIDException();

		// Put a get partial message downstream
		queryInfo.queryResult.returnPartialResults();
		break;
            
            case RequestMessage.RUN_GC:
	    	    System.out.println("Starting Garbage Collection");
		    long startime = System.currentTimeMillis();
                    System.gc();
		    long stoptime = System.currentTimeMillis();
		    double executetime = (stoptime-startime)/1000.0;
		    System.out.println("Garbage Collection Completed." +
		    " Time: " + executetime + " seconds.");
	            ResponseMessage doneMesg = 
		         new ResponseMessage(request,
			               ResponseMessage.END_RESULT);
	            sendResponse(doneMesg);
                    break;

            case RequestMessage.SHUTDOWN:
		System.out.println("Shutdown message received");
	            ResponseMessage shutMesg = 
		         new ResponseMessage(request,
			               ResponseMessage.END_RESULT);
	            sendResponse(shutMesg);
		    // boy this is ugly
		    System.exit(0);
		    break;
		//-------------------------------------
		//   Ooops 
		//-------------------------------------
	    default:
		System.out.println("ConnectionThread: INVALID_REQUEST");
		break;		
	    }

	} catch(InvalidQueryIDException e) {
	    System.out.println("KT: Sending error message to client");
	    ResponseMessage errMesg = 
		new ResponseMessage(request,ResponseMessage.INVALID_SERVER_ID);
	    sendResponse(errMesg);
	} catch (XMLQueryPlanParser.InvalidPlanException ipe) {
	    // KT THIS and other exceptions should be sent back
	    // to client instead of being handled here...!!!
	    System.out.println("Invalid Plan: " + ipe.getMessage());
	    sendResponse(new ResponseMessage(request, ResponseMessage.PARSE_ERROR));
	} catch(niagara.query_engine.QueryResult.ResultsAlreadyReturnedException re) {
	    System.err.println("RequestHandler: ResultsAlready Returned " + re.getMessage());
	    re.printStackTrace();
	} catch (niagara.query_engine.QueryResult.AlreadyReturningPartialException arpe) {
	    System.err.println("RequestHandler: AlreadyReturningPartial: " 
			       + arpe.getMessage());
	    arpe.printStackTrace();
	} catch(niagara.utils.ShutdownException se) {
	    System.err.println("Shutdown exception received in RequestHandler.handleRequest " + se.getMessage());
	}
    }    
    
    /**Method used by everyone to send responses to the client
       @param mesg The message that needs to be sent
    */
    public synchronized void sendResponse(ResponseMessage mesg) {
	try {
            outputWriter.write(mesg.toXML());
	    outputWriter.flush();
	}
	catch (IOException e) {
	    //looks like connection stream got disconnected
	    //gracefully shutdown the whole client connection
            System.out.println("XXX sendResponse got an IOException - closing client connection (KT)");
	    closeConnection();
	}
    }

    /**
     *  Kill the query with id = queryID and return a response
     *
     *  @param queryID the id of the query to kill
     */
    public void killQuery(int queryID) throws InvalidQueryIDException
    {
	// Get the queryInfo object for this request
	ServerQueryInfo queryInfo = queryList.get(queryID);
	
	// Respond to an invalid queryID
	if (queryInfo == null) {
            throw new InvalidQueryIDException();
	}

	// Process Kill message
	// Remove the query from the active queries list
	queryList.remove(queryID);
	
	// destroy the transmitter thread
	queryInfo.transmitter.destroy();
	
	// if it is a trigger then contact trigger manager
	if (queryInfo.isTriggerQuery()) {
	    server.triggerManager.deleteTrigger(queryInfo.triggerName);
	}
	else if (!queryInfo.isSEQuery())
	    // Put a KILL control message down stream
	    queryInfo.queryResult.kill();
    }

    /**
     *  Get the DTD list from DM (which contacts ther YP if list is not cached)
     * 
     */
    public Vector getDTDList()
    {
	Vector ret = null;
	ret = server.qe.getDTDList();
	return ret;
    }

    /**Gracefully shutdow the cunnection to this client
       cleans up all the outstanding queryies and triggers
    */
    public void closeConnection() {
	// first of all,kill all the queries
	Enumeration e = queryList.elements();
	while (e.hasMoreElements()) {
	    try {
		ServerQueryInfo info = (ServerQueryInfo) e.nextElement();
		killQuery(info.getQueryId());
	    } catch(InvalidQueryIDException iqe) {
		System.err.println("RequestHandler - invalid query id during closeConnection " + iqe.getMessage());
	    }
	}
	// and we are done!
    }

    /**Sends the DTD List to the client
       @param request The client sent this request
    */
    public void sendDTDList(RequestMessage request) {
	ResponseMessage resp = new ResponseMessage(request,ResponseMessage.DTD_LIST);
	Vector dtdlist = getDTDList();
	if (dtdlist == null)
	    resp.type = ResponseMessage.ERROR;
	else {
	    for (int i=0;i<dtdlist.size();i++) 
		resp.responseData += (String) dtdlist.elementAt(i) + "\n";
	}
	sendResponse(resp);
    }

    /**Send the queryId that has been assigned to this query. This is the first things that
       is sent to the client after a query is received
       @param request The initial request
    */
    private void sendQueryId(RequestMessage request) {
	ResponseMessage resp = new ResponseMessage(request,ResponseMessage.SERVER_QUERY_ID);
	sendResponse(resp);
    }

    /**Get a new query id
       @return new query id
    */
    public synchronized int getNextConnServerQueryId()
    { 
	return (lastQueryId++); 
    }

    class InvalidQueryIDException extends Exception {
    }

    /**Class for storing the ServerQueryInfo objects into a hashtable and accessing it
       Essentially a wrapper around Hashtable class with similar functionality
    */
    class QueryList {
	Hashtable queryList;
	
	public QueryList() {
	    queryList = new Hashtable();
	}
	
	public ServerQueryInfo get(int qid) {
	    return (ServerQueryInfo) queryList.get(new Integer(qid));
	}
	
	public ServerQueryInfo put(int qid,ServerQueryInfo info) {
	    return (ServerQueryInfo) queryList.put(new Integer(qid),info);
	}

	public ServerQueryInfo remove(int qid) {
	    System.out.println("KT: Query with ServerQueryId " + qid +
			       " removed from RequestHandler.QueryList");
	    return (ServerQueryInfo) queryList.remove(new Integer(qid));
	}

	public Enumeration elements() {
	    return queryList.elements();
	}
    }
}








