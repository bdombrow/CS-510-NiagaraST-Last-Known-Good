
/**********************************************************************
  $Id: ResultTransmitter.java,v 1.3 2000/08/23 03:55:02 tufte Exp $


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

import org.w3c.dom.*;
import com.ibm.xml.parser.*;
import java.io.*;
import niagara.query_engine.*;
import java.net.*;

import niagara.utils.*;
import niagara.data_manager.DataManager;

/** This is the thread for transmitting out the results of a query to the 
 * client Every query that gets submitted to the server has a corresponding 
 * resultTranmitting thread 
 */

public class ResultTransmitter implements Runnable {

    // The queryInfo of the query to which this transmitter belongs
    private ServerQueryInfo queryInfo;
    
    private RequestMessage request;
    
    // this will be used to access the output stream to send out results
    private RequestHandler handler;
    
    // The results that are yet to be sent
    private int totalResults;
    
    // The thread this guy runs on 
    private Thread transmitThread;

    // for suspending and resuming the thread
    private boolean suspended = false;

    // whether this thread is scheduled for killing
    private boolean killThread = false;

    // The response message in construction
    private ResponseMessage response;
    
    // The number of results that have been collected so far in 'response'
    private int resultsSoFar = 0;
    
    // The size of batch in a batched query
    private static final int BatchSize=50;
    
    // Tags for element and attribute list
    private static final String ELEMENT = "<!ELEMENT";
    private static final String ATTLIST = "<!ATTLIST";
    
    /** Constructor
	@param handler The request handler that created this transmitter
	@param queryInfo The info about the query to be executed
	@param request The request that contained the query
    */
    public ResultTransmitter(RequestHandler handler, ServerQueryInfo queryInfo,
			     RequestMessage request) {
	this.handler=handler;
	this.queryInfo = queryInfo;
	this.request = request;
	totalResults = 0;
	transmitThread = new Thread(this,"ResultTransmitter:"+queryInfo.getQueryId());
	transmitThread.start();
    }
    
    public void run() {
	if (queryInfo.isSEQuery()) 
	    handleSEQuery();
	else if(queryInfo.isTriggerQuery())
	    handleTriggerQuery();
	else if(queryInfo.isQEQuery())
	    handleQEQuery();
	else if(queryInfo.isDTDQuery()) 
	    handleDTDQuery();
	else if(queryInfo.isAccumFileQuery()) 
	    handleAccumQuery();
	else 
	    throw new PEException("Invalid query type");
    }
    
    private void handleDTDQuery() {
    	ResponseMessage response = new ResponseMessage(request,ResponseMessage.DTD);
	try {
	    URL url = new URL(request.requestData);
	    BufferedReader rd = new BufferedReader(new InputStreamReader(url.openStream()));
	    response.responseData = "<![CDATA[";
	    //  	    while (rd.ready()) {
	    //  		response.responseData += rd.readLine()+"\n";
	    //  	    }
	    response.responseData += assembleDTD(rd) + "]]>";
	    //  	    response.responseData += "]]>";
	}
	catch (MalformedURLException e1) {
	    response = new ResponseMessage(request,ResponseMessage.ERROR);
	    response.responseData = "Bad Url for DTD";
	}
	catch (IOException e2) {
	    response = new ResponseMessage(request,ResponseMessage.ERROR);
	    response.responseData = "Could Not Fetch the DTD";
	}
	handler.sendResponse(response);
    }
    
    /**
     * helper to handle DTD Query
     */
    private String assembleDTD(BufferedReader r) throws IOException
    {
	StringBuffer res = new StringBuffer();
	
	while(r.ready()){
	    String line = r.readLine();
	    
	    if(line.indexOf(ELEMENT) != -1 ||
	       line.indexOf(ATTLIST) != -1 ||
	       line.indexOf("<!ENTITY") != -1){
		// add the line to the dtd
		res.append(line);
		// if the end > is not on this line
		// continue to add lines until you find the >
		if(line.indexOf(">") == -1){
		    while(r.ready()){
			line = r.readLine();
			res.append(line);
			if(line.indexOf(">") != -1){
			    break;
			}
		    }
		}
		res.append("\n");
	    }
	    
	}
	return res.toString();
    }
    
    /** Handles SE Query.
	Contacts the SE Server and sends back results to the client 
    */
    private void handleSEQuery() {
	// submit the query to the client
	String result = handler.server.seClient.query(queryInfo.searchEngineQuery);
	// create the response message
	ResponseMessage response = new ResponseMessage(request,ResponseMessage.SE_QUERY_RESULT);
	response.responseData = result;
	handler.sendResponse(response);
	// send the end result saying everything is in
	handler.sendResponse(new ResponseMessage(request,ResponseMessage.END_RESULT));
	try {
	    handler.killQuery(queryInfo.getQueryId());
	}
	catch  (RequestHandler.InvalidQueryIDException e) {
	    // do nothing
	    // this will never happen by the way
	}
	return;
    }
    
    /** Handles QE Query.
     */
    private void handleQEQuery() 
    {
	QueryResult queryResult = queryInfo.queryResult;
	
	while (true) {
	    // if this thread has been marked for committing suicide, do it
	    if (killThread) 
		return;
	    
	    response = 
		new ResponseMessage(request,
				    ResponseMessage.QUERY_RESULT);
	    
	    // see if the transmitter is currently suspended
	    // It could be becuase of two reasons
	    // 1. No Pending Requests 2. Last results were suspended
	    boolean suspend = checkSuspension();		    
	    
	    // if we are going to suspend, better send the results collected so far
	    if (suspend)
		sendResults();
	    
	    // as long as atleast one suspension condition is true, keep waiting
	    while (checkSuspension())
		doWait();
	    
				// if this thread is scheduled for killing
	    if (killThread)
		return;
	    
	    QueryResult.ResultObject resultObject;
	    
				//get the next result
	    try {
		resultObject = queryResult.getNext(2000);
	    }
	    catch (QueryResult.ResultsAlreadyReturnedException e) {
		return;
	    }
	    
	    switch (resultObject.status) {
		// If this was the last stream element this query is done
	    case QueryResult.EndOfResult:
		try{
		    sendResults();
		    // send the end result response
		    response = 
			new ResponseMessage(request,
					    ResponseMessage.END_RESULT);
		    handler.sendResponse(response);
		    
		    //everything done! kill the query
		    handler.killQuery(request.serverID);
		    
		    return;
		}
		catch(RequestHandler.InvalidQueryIDException nsqe){ 
		    // Should never get here
		    nsqe.printStackTrace();
		}
		break;
		
		// If it is just a regular query result save it
	    case QueryResult.FinalQueryResult:
	    case QueryResult.PartialQueryResult:
		// add the result to responseData
		totalResults--;
		resultsSoFar++;
		if (resultsSoFar > BatchSize)
		    sendResults();
		response.responseData += getResultData(resultObject);
		handler.sendResponse(response);
		break;
		
	    case QueryResult.QueryError:
		processError();
		break;
		
		// if no more new results have come in a while
	    case QueryResult.TimedOut:
		sendResults();
		break;
	    }
	    
	}
    }
    
    
    /** Handles Trigger Query.
	Hands over the query to trigger manager and listens on the 
		query result queue for a query result for each firing
    */
    private void handleTriggerQuery() {
		// first get the query result
	QueryResult queryResult = (QueryResult) queryInfo.queryResultQueue.get();
	while (true) {
	    
	    // if this thread has been marked for committing suicide, do it
	    if (killThread) 
		return;
	    
	    ResponseMessage response = 
		new ResponseMessage(request,
				    ResponseMessage.QUERY_RESULT);
	    
	    QueryResult.ResultObject resultObject;
	    
	    //get the next result
	    try {
		resultObject = queryResult.getNext();
	    }
	    // If something goes wrong, kill this query and move on to the next query result
	    catch (InterruptedException e) {
		queryResult.kill();
		queryResult = (QueryResult) queryInfo.queryResultQueue.get();
		continue;
	    }
	    catch (QueryResult.ResultsAlreadyReturnedException e) {
		queryResult.kill();
		queryResult = (QueryResult) queryInfo.queryResultQueue.get();
		continue;
	    }
	    
	    // If this was the last stream element this query is done
	    switch (resultObject.status) {
	    case QueryResult.EndOfResult:
				// send the end result response
		response = 
		    new ResponseMessage(request,
					ResponseMessage.END_RESULT);
		handler.sendResponse(response);
		queryResult.kill();
		queryResult = (QueryResult) queryInfo.queryResultQueue.get();
		break;
		
	    case QueryResult.FinalQueryResult:
	    case QueryResult.PartialQueryResult:
				// add the result to responseData
		totalResults--;
		response.responseData = getResultData(resultObject);
		handler.sendResponse(response);
		break;
		
	    case QueryResult.QueryError:
		processError();
		queryResult.kill();
		queryResult = (QueryResult) queryInfo.queryResultQueue.get();
		break;		
	    }

	}
    }

    /** 
     * Function to "handle" an Accumulate Query.  This function
     * is run in this class's thread and basically sits on top
     * of a Accumulate Query.  The function of this function/thread
     * is to pull results from the accumulate operator and update
     * the Accumulate File Directory.
     */
    private void handleAccumQuery() {
	QueryResult queryResult = queryInfo.queryResult;
	QueryResult.ResultObject resultObject;
	boolean alreadyReturningPartial = false;

	/* give the query some time to get started */
	/* 	transmitThread.sleep(5000); */

	while (true) {

	    /* If a kill query message has been sent, killThread will
	     * be set to true. So, return to end this thread's execution
	     */
	    if (killThread) {
		/* don't think I want to do this - client has to remove
		 * accum file when ready
		 */
		/* DataManager.AccumFileDir.remove(queryInfo.accumFileName); */
		return;
	    }
	    
	    /* I removed all the suspension stuff and made it so that
	     * AccumFile queries can't be suspended because I don't
	     * fully understand suspension and because it doesn't
	     * seem necessary for AccumFile stuff
	     */

	    try {
		if(!alreadyReturningPartial) {
		    /* sleep for some amount of time */
		    /* there are two thread sleep functions 
		     * sleep(long millis), sleep(long millis, int nanos)
		     */
		    transmitThread.sleep(10000);
		    
		    /* request the generation of partial results - 
		     * who knows if this will work or not
		     */
		    //System.out.println("Accum Mgr requesting partial result");
		    queryInfo.queryResult.returnPartialResults();
		}

		alreadyReturningPartial = false;
		
		/* get the result and update the accum file dir */
		/* OK, now wait for the result to come popping up */
		resultObject = queryResult.getNext();
		
		//System.out.println("Accum Mgr received result");
		switch (resultObject.status) {
		case QueryResult.PartialQueryResult:
		    /* In this case, resultObject.result is a TXDocument 
		     * AccumFileDir stores standard DOM Docs, since that
		     * is what the system uses now 
		     */
		    System.out.println("Updating Accumulate File " +
		    		       queryInfo.accumFileName);
		    DataManager.AccumFileDir.put(queryInfo.accumFileName, 
						 resultObject.result);
		    break;

		case QueryResult.FinalQueryResult:
		    /* In this case, resultObject.result is a TXDocument 
		     * AccumFileDir stores standard DOM Docs, since that
		     * is what the system uses now 
		     */
		    //System.out.println("Updating Accumulate File " +
		    //		       queryInfo.accumFileName);
		    DataManager.AccumFileDir.put(queryInfo.accumFileName, 
						 resultObject.result);

		    /* send final results to client */
		    response = 
			new ResponseMessage(request,
					    ResponseMessage.QUERY_RESULT);
		    response.responseData += getResultData(resultObject);
		    handler.sendResponse(response);

		    break;
		    
		case QueryResult.QueryError:
		    processError();
		    break;

		case QueryResult.EndOfResult:
		    /* send the end result response */
		    response = 
			new ResponseMessage(request,
					    ResponseMessage.END_RESULT);
		    handler.sendResponse(response);
		
		    /* everything done! kill the query */
		    handler.killQuery(request.serverID);
		    return;

		case QueryResult.EndOfPartialResult:
		    /* think I can ignore this */
		    System.out.println("ResultTransmitter.handleAccumQuery ignoring EndOfPartialResult");
		    break;

		    case QueryResult.NonBlockingResult:
		case QueryResult.TimedOut:
		    /* should only get partial results, something
		     * is wrong if I get one of the other statuses, I think
		     */
		    throw new PEException("Unexpected QueryResult status" +
					  String.valueOf(resultObject.status));
		    
		default:
		    throw new PEException("Unexpected QueryResult status");
		}
	    } catch (QueryResult.ResultsAlreadyReturnedException e) {
		throw new PEException("HELP - What happened??");
	    } catch (InterruptedException e) {
		System.out.println("WARNING: Accumulate File Result Transmitter Thread interrupted!!");
		/* do nothing, just continue??? */
	    } catch (QueryResult.AlreadyReturningPartialException e) {
		alreadyReturningPartial = true;

	    } catch (RequestHandler.InvalidQueryIDException e) {
		throw new PEException("Invalid query id found in ResultTrans");
	    }
	}
    }
    

    /**handle a request for more elements
       This method is valid only for Query Engine queries
       It is called whenever the user does getNext
       @param request The request message that came from the client
    */
    synchronized public void handleRequest(RequestMessage request) {
	this.request = request;
	totalResults += Integer.parseInt(request.requestData);
	suspended = false;
	notify();
    }

    // Just a synchronized wrapper around wait()
    synchronized private void doWait() {
	try {
	    wait();
	}
	catch (InterruptedException e) {
	    System.out.println("Waiting Thread Interrupted");
	}
    }

    /**Suspend this transmitter
     */
    synchronized public void suspend() {
	if(queryInfo.isAccumFileQuery()) {
	    throw new PEException("Can't suspend an Accumulate File Query");
	}
	suspended = true;
    }

    /**Resume suspended transmission
     */
    synchronized public void resume() {
	suspended = false;
	notify();
    }

    /**Check whether the transmission should be suspended for any reason
     */
    synchronized private boolean checkSuspension() {
	// triggers are never suspended (for now)
	if (queryInfo.isTriggerQuery())
	    return false;
	if (suspended)
	    return true;
	if (totalResults <= 0)
	    return true;
	return false;
    }

    /**Extract the XML string from the result object
     */
    private String getResultData(QueryResult.ResultObject ro) {
	StringWriter writer = new StringWriter();
	try {
	    ro.result.printWithFormat(writer);
	}
	catch (IOException e) {
	    e.printStackTrace();
	}
	return writer.toString();
    }

    
    /**
     * Shut down the query in the event of serious error
     *
     */

    private void processError () {
	System.out.println("Request for shut down. Sending error message");
	ResponseMessage response = new ResponseMessage(request,ResponseMessage.ERROR);
	response.responseData = "Internal Error in Query Engine";
	handler.sendResponse(response);
	// there is no purpose left in my life. I should die
	destroy();
    }

    // set the kill flag
    public void destroy() { 
	killThread = true;
    }

    // send the results collected so far
    private void sendResults() {
	if (!response.responseData.equals(""))
	    handler.sendResponse(response);
	response.responseData = "";
	resultsSoFar = 0;
    }
}
