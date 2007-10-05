/**********************************************************************
  $Id: RequestHandler.java,v 1.40 2007/10/05 20:45:28 vpapad Exp $


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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.management.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import niagara.connection_server.RequestMessage.InvalidRequestTypeException;
import niagara.data_manager.ConstantOpThread;
import niagara.data_manager.StreamThread;
import niagara.optimizer.Optimizer;
import niagara.optimizer.Plan;
import niagara.optimizer.TracingOptimizer;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Op;
import niagara.physical.PhysicalAccumulate;
import niagara.physical.Tunable;
import niagara.query_engine.Instrumentable;
import niagara.query_engine.QueryInfo;
import niagara.query_engine.QueryResult;
import niagara.query_engine.QueryResult.AlreadyReturningPartialException;
import niagara.utils.*;

/**There is one request handler per client and receives all the requests from that client
   Then that request is further dispatched to the appropriate module and results sent back
*/
public class RequestHandler {
    // Hashtable of queries
    private QueryList queryList;

    // The parser which listens to the stream coming from client
    RequestParser requestParser;
    // The Writer to which all the results have to go
    private BufferedWriter outputWriter;

    // The server which instantiated this RequestHandler
    NiagraServer server;

    // Every query is given a server query id. This is the counter for giving out that service id
    private int lastQueryId = 0;

    private XMLQueryPlanParser xqpp;
    private Optimizer optimizer;
    private CPUTimer cpuTimer;
    
    private Catalog catalog;
    
    /** Do we need to send each response message as a separate XML document,
     *  with its own header? */
    private boolean sendHeader;
    private static String xmlHeader = "Content-type: application/xml\n\n<?xml version='1.0'?>\n";
    /** The delimiter between response messages */
    private static String delimiter = "\n--<][>\n";
    /** The footer sent after the last response */
    private static String footer = "\n--<][>--\n";
    
    private boolean connectionClose = false;

    private static HashMap<Long, Long> threadCPUTimes = new HashMap<Long, Long>();

    /**Constructor
       @param sock The socket to read from 
       @param server The server that has access to other modules
    */
    public RequestHandler(Socket sock, NiagraServer server)
        throws IOException {
        this.outputWriter =
            new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
        this.server = server;

        initialize();

        sendBeginDocument();

        // Request parser will take care of the rest through callbacks
        this.requestParser = new RequestParser(sock.getInputStream(), this);
        this.requestParser.startParsing();
    }

    private void initialize() throws IOException {
        this.catalog = NiagraServer.getCatalog();

        // A hashtable of queries with qid as key
        this.queryList = new QueryList();

        this.xqpp = new XMLQueryPlanParser();
        this.optimizer = new Optimizer();
        // XXX vpapad: uncomment next line to get the tracing optimizer
        //this.optimizer = new TracingOptimizer();
    }
    
    public RequestHandler(String queryString,
            String requestType,
            HttpServletResponse res, 
            NiagraServer server) throws IOException {
        this.outputWriter =
            new BufferedWriter(new OutputStreamWriter(res.getOutputStream()));
        this.server = server;
        this.sendHeader = true;
        
        initialize();

        this.xqpp = new XMLQueryPlanParser();
        this.optimizer = new Optimizer();
        
        // Send initial delimiter
        outputWriter.write(delimiter);
        
        RequestMessage request = new RequestMessage();
        try {
            request.setRequestType(requestType);
            // XXX vpapad: Do we ever need sendImmediate=false 
            // on the HTTP interface?
            request.setSendImmediate(true);
        } catch (InvalidRequestTypeException e) {
            sendErrMessage(request, ResponseMessage.ERROR, "Invalid request type" + requestType);
            return;
        }
        request.requestData = queryString;
        handleRequest(request);
        waitForCompletion(request);
    }
    
    private void waitForCompletion(RequestMessage request) {
        ServerQueryInfo qi = queryList.get(request.serverID); 
        if (qi == null)
            return;
        Object queryDone = qi.getTransmitter().getQueryDone();
        while (true) {
            synchronized(queryDone) {
                try {
                    queryDone.wait();
                    return;
                } catch (InterruptedException ie) {
                    ;
                }
            }
        }
    }
    
    // Send the initial string to the client
    private void sendBeginDocument() throws IOException {
        String header = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n<response>\n";
        outputWriter.write(header);
        outputWriter.flush();
    }

    public void handleRequest(RequestMessage request) {
        Plan plan, optimizedPlan;

        if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
            if (cpuTimer == null)
                cpuTimer = new CPUTimer();
            cpuTimer.start();
        }

        boolean error = false;
        int err_type = 0;
        String message = "";
        try {
            // Handle the request according to requestType
            switch (request.getRequestType()) {
                case EXECUTE_QP_QUERY:
                    plan = xqpp.parse(request.requestData);
                    // Optimize the plan
                    optimizedPlan = null;
                    try {
                        optimizedPlan = optimizer.optimize(plan.toExpr());
                    } catch (Exception e) {
                        System.err
                                .println("XXX vpapad: exception occured during optimization");
                        e.printStackTrace();
                        assert false;
                    }
                    xqpp.clear();
                    assignQueryId(request);
                    processQPQuery(optimizedPlan, request);
                    break;

                case EXECUTE_PREPARED_QUERY:
                    String pID = request.requestData.trim();
                    ServerQueryInfo sqi = catalog.getQueryInfo(pID);
                    
                    if (sqi != null) {
                        ResultTransmitter trans = sqi.getTransmitter();
                        request.serverID = sqi.getQueryId();
                        sendQueryId(request);
                        queryList.put(request.serverID, sqi);
                    	trans.setHandler(this);
                        if (request.isIntermittent()) {
                        	fetchEpoch(request, sqi);
                        }

                    } else {
	                    optimizedPlan = catalog.getPreparedPlan(pID);
	                    if (optimizedPlan == null) {
	                        sendErrMessage(request, ResponseMessage.ERROR, 
	                        		"Non-existent prepared query id: " + pID);                        
	                    } else {
	                        optimizedPlan.setPlanID(pID);
	                        assignQueryId(request);
	                        sqi = processQPQuery(optimizedPlan, request);
	                        catalog.registerQueryInfo(pID, sqi);
	                        if (request.isIntermittent()) {
	                        	fetchEpoch(request, sqi);
	                        }
	                    }
                    }
                    
                    break;
                    
                case KILL_PREPARED_QUERY:
                    String planid = request.requestData.trim();
                    ServerQueryInfo query = catalog.getQueryInfo(planid);
                    if (query != null) {
                        ResultTransmitter trans = query.getTransmitter();
                        request.serverID = query.getQueryId();
                        sendQueryId(request);
                        queryList.put(request.serverID, query);
                       	
                        if (trans.getHandler().isConnectionOpen() ) {
	                        trans.getHandler().sendResponse(new ResponseMessage(request, 
	         						 ResponseMessage.END_RESULT));
	                        trans.getHandler().killQuery(request.serverID);
                        }
                    	//trans.setHandler(this);
                       	sendResponse(new ResponseMessage(request, 
        						 ResponseMessage.END_RESULT));

                        killQuery(request.serverID);
                        break;

                    } else {
	                    Plan prepared = catalog.getPreparedPlan(planid);
	                    catalog.removePreparedPlan(planid);
	                    
	                    assignQueryId(request);
	                    
	                    if (prepared == null ) {
	                    	sendErrMessage(request, ResponseMessage.ERROR, 
	                        		"Non-existent prepared query id: " + planid);
	                    }  else  {
	                       	sendResponse(new ResponseMessage(request, 
	       						 ResponseMessage.END_RESULT));
	                    }
	                    // We don't want to actually *run* the plan!
	                    // Replace the plan with a constant operator
	                    // having the prepared plan as its content
	                    
	                    optimizedPlan = new Plan(new ConstantOpThread(
	                            "<kill_prepared id='" + planid +"'/>", new Attrs()));

	                    processQPQuery(optimizedPlan, request);
                    }
                	break;

                case MQP_QUERY:
                    plan = xqpp.parse(request.requestData);
                    new MQPHandler(server.qe.getScheduler(), optimizer, plan);
                    xqpp.clear();
                    break;

                case EXECUTE_QE_QUERY:
                    // assign a new query id to this request
                    int qid = getNextConnServerQueryId();

                    // create and populate the query info
                    ServerQueryInfo queryInfo = new ServerQueryInfo(qid,
                            ServerQueryInfo.QueryEngine);

                    // start the transmitter thread for sending results back
                    ResultTransmitter transmitter = new ResultTransmitter(this,
                            queryInfo, request);
                    queryInfo.setTransmitter(transmitter);

                    // now give the query to the query engine
                    server.qe.executeQuery(transmitter, queryInfo,
                            request.requestData);
                    request.serverID = qid;
                    sendQueryId(request);

                    queryList.put(qid, queryInfo);
                    break;

                case SUSPEND_QUERY:
                    throw new InvalidPlanException(
                            "Query suspension no longer allowed");

                case RESUME_QUERY:
                    throw new InvalidPlanException(
                            "Query suspension no longer allowed");

                case KILL_QUERY:
                    String id = request.requestData.trim();                 
                    Plan prepared = catalog.getPreparedPlan(id);

                	if (prepared!=null) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                        		"Kill prepared query with kill-prepared");                        
                	} else {
                    killQuery(request.serverID);
                	}
                    break;

                case GET_NEXT:
                    // get the queryInfo of this query
                    queryInfo = queryList.get(request.serverID);

                    // Respond to invalid queryID
                    if (queryInfo == null) {
                        if (queryList.queryWasRun(request.serverID)) {
                            break;
                        } else {
                            assert false : "Bad query id " + request.serverID;
                        }
                    }

                    queryInfo.getTransmitter().handleRequest(request);

                    break;

                case GET_PARTIAL:
                    // Get the queryInfo object for this request
                    queryInfo = queryList.get(request.serverID);

                    // Respond to invalid queryID
                    if (queryInfo == null)
                        assert false : "Bad query id " + request.serverID;

                    // Put a get partial message upstream
                    queryInfo.getQueryResult().requestPartialResult();
                    break;

                case RUN_GC:
                    System.out.println("Starting Garbage Collection");
                    long startime = System.currentTimeMillis();
                    System.gc();
                    long stoptime = System.currentTimeMillis();
                    double executetime = (stoptime - startime) / 1000.0;
                    System.out.println("Garbage Collection Completed."
                            + " Time: " + executetime + " seconds.");
                    ResponseMessage doneMesg = new ResponseMessage(request,
                            ResponseMessage.END_RESULT);
                    sendResponse(doneMesg);
                    break;

                case DUMPDATA:
                    if (NiagraServer.RUNNING_NIPROF) {
                        System.out.println("Requesting profile data dump");
                        JProf.requestDataDump();
                        ResponseMessage doneDumpMesg = new ResponseMessage(
                                request, ResponseMessage.END_RESULT);
                        sendResponse(doneDumpMesg);
                    } else {
                        System.out
                                .println("Profiler not running - unable to dump data");
                        ResponseMessage errMesg = new ResponseMessage(request,
                                ResponseMessage.ERROR);
                        errMesg
                                .setData("Profiler not running - unable to dump data");
                        sendResponse(errMesg);
                    }
		    showCPUTimes();
                    break;
                case SHUTDOWN:
                    System.out.println("Shutdown message received");
                    ResponseMessage shutMesg = new ResponseMessage(request,
                            ResponseMessage.END_RESULT);
                    sendResponse(shutMesg);
                    System.exit(0); 
                    break;
                case SYNCHRONOUS_QP_QUERY:
                    plan = xqpp.parse(request.requestData);

                    // Optimize the plan
                    optimizedPlan = null;
                    try {
                        optimizedPlan = optimizer.optimize(plan.toExpr());
                    } catch (Exception e) {
                        System.err
                                .println("exception occured during optimization");
                        e.printStackTrace();
                    }
                    xqpp.clear();

                    processQPQuery(optimizedPlan, request);
                    // get the queryInfo of this query
                    queryInfo = queryList.get(request.serverID);
                    queryInfo.getTransmitter().handleSynchronousRequest();
                    break;
                case EXPLAIN_QP_QUERY:
                    plan = xqpp.parse(request.requestData);

                    // Optimize the plan
                    optimizedPlan = null;
                    try {
                        optimizedPlan = optimizer.optimize(plan.toExpr());
                    } catch (Exception e) {
                        System.err
                                .println("exception occured during optimization");
                        e.printStackTrace();
                    }

                    xqpp.clear();

                    // We don't want to actually *run* the plan!
                    // Replace the plan with a constant operator
                    // having the optimized plan as its content
                    optimizedPlan = new Plan(new ConstantOpThread(optimizedPlan
                            .planToXML(), new Attrs()));

                    assignQueryId(request);
                    processQPQuery(optimizedPlan, request);
                    break;
                case PREPARE_QUERY:
                    plan = xqpp.parse(request.requestData);

                    // Optimize the plan
                    optimizedPlan = null;
                    try {
                        optimizedPlan = optimizer.optimize(plan.toExpr());
                    } catch (Exception e) {
                        System.err.println("Exception occured during optimization");
                        server.shutdown();
                    }

                    xqpp.clear();

                    // Store the prepared plan and register its operators
                    // for instrumentation queries
                    optimizedPlan.setPlanID(plan.getPlanID());
                    String planID = catalog.storePreparedPlan(optimizedPlan);
                    String instrumentedPlanAsXML = 
                        catalog.instrumentPlan(planID);
                    
                    // We don't want to actually *run* the plan!
                    // Replace the plan with a constant operator
                    // having the prepared plan as its content
                    optimizedPlan = new Plan(new ConstantOpThread(
                            instrumentedPlanAsXML, new Attrs()));

                    assignQueryId(request);
                    processQPQuery(optimizedPlan, request);
                    break;
                    
                case SET_TUNABLE:
                    // requestData must have the form:
                    // planID.operator.tunable=value
                    String str = request.requestData.trim();
                    String[] parts = str.split("\\.");
                    if (parts.length != 3) {
                        sendErrMessage(request, ResponseMessage.ERROR, "Invalid syntax in SET_TUNABLE request");
                        break;
                    } 
                    // Get planID
                    planID = parts[0];
                    plan = catalog.getPreparedPlan(planID);
                    if (plan == null) {
                        sendErrMessage(request, ResponseMessage.ERROR, "Non-existent prepared query id: " + planID);
                        break;
                    } 
                    // Get operator
                    String opID = parts[1];
                    Instrumentable operator = catalog.getOperator(planID, opID);
                    if (operator == null) {
                        sendErrMessage(request, ResponseMessage.ERROR, "Non-existent operator id: " + opID);
                        break;
                    }
                    // Get tunable
                    String rest = parts[2];
                    parts = rest.split("=", 2);
                    if (parts.length != 2) {
                        sendErrMessage(request, ResponseMessage.ERROR, "Invalid syntax in SET_TUNABLE request");
                        break;
                    }
                    String tunable = parts[0];
                    String newValue = parts[1];
                    String setterName = null;
                    Tunable.TunableType type = null;
                    for (Method m : operator.getClass().getMethods()) {
                        if (!m.isAnnotationPresent(Tunable.class))
                            continue;
                        Tunable t = m.getAnnotation(Tunable.class);
                        if (!t.name().equals(tunable))
                            continue;
                        setterName = t.setter();
                        type = t.type();
                    }                
                    if (setterName == null) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Unknown tunable: " + opID + "." + tunable);
                        break;
                    }
                    // Get setter method & invoke it
                    Method setter = null;
                    try {
                        switch (type) {
                            case BOOLEAN:
                                setter = operator.getClass().getMethod(setterName,
                                        boolean.class);
                                boolean b;
                                if (newValue.equals("true"))
                                    b = true;
                                else if (newValue.equals("false"))
                                    b = false;
                                else
                                    throw new IllegalArgumentException("Could not convert " + newValue + " to a boolean");
                                setter.invoke(operator, b);
                                break;
                            case INTEGER:
                                setter = operator.getClass().getMethod(setterName,
                                        int.class);
                                int i = Integer.parseInt(newValue);
                                setter.invoke(operator, i);
                                break;
                            default:
                                throw new PEException("Unexpected tunable type: "
                                        + type);
                        }
                    } catch (NoSuchMethodException e) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Could not find setter method for " + opID + "." + tunable);
                    } catch (SecurityException e) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Could not access setter method for " + opID + "." + tunable);
                    } catch (IllegalArgumentException e) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Problem invoking setter method for " 
                                + opID + "." + tunable + ": " + e.getMessage());
                    } catch (IllegalAccessException e) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Problem invoking setter method for " 
                                + opID + "." + tunable + ": " + e.getMessage());
                    } catch (InvocationTargetException e) {
                        sendErrMessage(request, ResponseMessage.ERROR, 
                                "Problem invoking setter method for " 
                                + opID + "." + tunable + ": " + e.getMessage());
                    }
                    assignQueryId(request);
                    sendResponse(new ResponseMessage(request,
                            ResponseMessage.END_RESULT));
                    break;
                // -------------------------------------
                // Ooops
                // -------------------------------------
                default:
                    throw new PEException("ConnectionThread: INVALID_REQUEST "
                            + request.getRequestType());
            }
        } catch (InvalidPlanException e) {
            error = true;
            err_type = ResponseMessage.PARSE_ERROR;
            message = e.getMessage();
        } catch (QueryResult.AlreadyReturningPartialException e) {
            error = true;
            err_type = ResponseMessage.EXECUTION_ERROR;
            message = e.getMessage();
        } catch (ShutdownException e) {
            error = true;
            err_type = ResponseMessage.EXECUTION_ERROR;
            message = "System was shutdown during query";
        } catch (IOException e) {
            error = true;
            err_type = ResponseMessage.ERROR;
            message = "IOException during query: " + e.getMessage();
        } catch (RuntimeException e) {
            // deal with runtime exceptions here - if not these are
            // turned into SAX exceptions by the parser
            sendErrMessage(request, ResponseMessage.ERROR,
                    "Programming or Runtime Error (see server for message)");
            System.err.println("WARNING: PROGRAMMING or RUNTIME ERROR "
                    + e.getMessage());
            e.printStackTrace();
            // keep compiler happy
            error = false;
            err_type = -1;
            message = null;
            // kill system as would be expected on a runtime exception
            server.shutdown();
        }

        if (error) {
            System.err.println("\nAn error occured during query parsing or execution." +
                    "Error Message: " + message + "\n");
            sendErrMessage(request, err_type, message);
        }

        if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
            cpuTimer.stop();
            cpuTimer.print("HandleRequest (" + request.getRequestType() + ")");
        }

    }

	/**
	 * @param request
	 * @param sqi
	 * @throws IOException
	 */
	private void fetchEpoch(RequestMessage request, ServerQueryInfo sqi) throws IOException {
		// get the most recent epoch;
		ResponseMessage epoch = sqi.getTransmitter().getMostRecentEpoch();
		
		if (epoch != null) {		
			if (epoch.dataSize() != 0) {
			 	sendResponse(epoch);
			}
		}
		
		// Disconnect the connection.
		// The client side is supposed to close the connection on
		// receiving the END_RESULT msg;
		disconnectIntermittentConnection(request, sqi.getQueryId());
	}

    private void assignQueryId(RequestMessage request) throws IOException {
        request.serverID = getNextConnServerQueryId();
        sendQueryId(request);
    }

    private ServerQueryInfo processQPQuery(Plan plan, RequestMessage request)
        throws InvalidPlanException, ShutdownException, IOException {
        // XXX vpapad: commenting out code is a horrible sin
        // if (type.equals("submit_subplan")) {
        // // // The top operator better be a send...
        // // // XXX vpapad: Argh... Plan or logNode?
        // // SendOp send = (SendOp) ((logNode) top).getOperator();
        //                //
        //                //                String query_id = nextId();
        //                //                send.setQueryId(query_id);
        //                //                send.setCS(this);
        //                //                send.setSelectedAlgoIndex(0);
        //                //
        //                //                queries.put(query_id, send);
        //                //                
        //                //                out.println(query_id);
        //                //                out.close();
        //            } 
        //            } else if (type.equals("submit_distributed_plan")) {
        //                handleDistributedPlans(xqpp.getRemotePlans());
        //            } else {

        int qid = request.serverID;

        boolean isSynchronous =
            (request.getRequestType() == RequestMessage.RequestType.SYNCHRONOUS_QP_QUERY);

        /* create and populate the query info
         */
        ServerQueryInfo serverQueryInfo;
        Op top = plan.getOperator();

        if (top instanceof PhysicalAccumulate) {
            PhysicalAccumulate pao = (PhysicalAccumulate) top;
            serverQueryInfo =
                new ServerQueryInfo(
                    qid,
                    ServerQueryInfo.AccumFile,
                    isSynchronous);
            serverQueryInfo.setAccumFileName(pao.getAccumFileName());
        } else {
            serverQueryInfo =
                new ServerQueryInfo(
                    qid,
                    ServerQueryInfo.QueryEngine,
                    isSynchronous);
        }
        boolean stpp = true;

        if (top instanceof StreamThread) {
            stpp = ((StreamThread) top).prettyprint();
        }

        QueryResult qr = server.qe.getNewQueryResult();
        serverQueryInfo.setQueryResult(qr);
        queryList.put(qid, serverQueryInfo);

        // start the transmitter thread for sending results back
        ResultTransmitter transmitter =
            new ResultTransmitter(this, serverQueryInfo, request);
        serverQueryInfo.setTransmitter(transmitter);
        if (top instanceof StreamThread && stpp) {
            transmitter.setPrettyprint(false);
        }

        // KT - DO THIS LAST - otherwise we get unexpectedly null
        // transmitter!!!
        server.qe.execOptimizedQuery(transmitter, plan, qr);
        return serverQueryInfo;
    }

    /**Method used by everyone to send responses to the client
       @param mesg The message that needs to be sent
    */
    public synchronized void sendResponse(ResponseMessage mesg)
        throws IOException {
        if (sendHeader)
            outputWriter.write(xmlHeader);
        
        ServerQueryInfo sqi = queryList.get(mesg.getServerID());
        boolean padding = true; // is this the correct default?
        if (sqi != null) {
            padding = !sqi.isSynchronous();
        }
        mesg.toXML(outputWriter, padding);
        
        if (!sendHeader && mesg.type == ResponseMessage.END_RESULT)
            outputWriter.write("</response>\n");
        
        mesg.clearData();
        
        if (sendHeader)
            if (mesg.isFinal())
                outputWriter.write(footer);
            else
                outputWriter.write(delimiter);
        
        outputWriter.flush();
    }

    /**
     *  Kill the query with id = queryID and return a response
     *
     *  @param queryID the id of the query to kill
     */
    public void killQuery(int queryID) {    
        // Get the queryInfo object for this request
        ServerQueryInfo queryInfo = queryList.get(queryID);

        // Respond to an invalid queryID
        assert queryInfo != null : "Bad query id " + queryID;

        if (queryInfo == null) {
        	return;
        }
        // Process Kill message
        // Remove the query from the active queries list
        queryList.remove(queryID);

        // destroy the transmitter thread
        assert queryInfo.getTransmitter()
            != null : "KT way bad transmitter is null";
        if (queryInfo.getTransmitter() != null)
        	queryInfo.getTransmitter().destroy();

        // Put a KILL control message down stream
        queryInfo.getQueryResult().kill();

        if (queryInfo.isSynchronous()) {
            try {
            	outputWriter.flush();
                outputWriter.close();
                connectionClose = true;
            } catch (IOException e) {
                ; // XXX vpapad: don't know what to do here
            }
        }
    }

    /**Gracefully shutdow the cunnection to this client
       cleans up all the outstanding queryies 
    */
    public void closeConnection() {
        // first of all,kill all the queries
        Enumeration e = queryList.elements();
        while (e.hasMoreElements()) {
            ServerQueryInfo info = (ServerQueryInfo) e.nextElement();
            killQuery(info.getQueryId());
        }
        // and we are done!
    }

    public void disconnectIntermittentConnection(RequestMessage request, int queryID) throws IOException {
    	//ServerQueryInfo sqi = queryList.remove(queryID);
	//assert sqi != null;
		// send end of result msg;
		sendResponse(new ResponseMessage(request, 
			 ResponseMessage.END_RESULT));
		
    	outputWriter.flush();
    	//outputWriter.close();
    	connectionClose = true;
    }
    
    public boolean isConnectionOpen () {
    	return !connectionClose;
    }
    /**Send the queryId that has been assigned to this query. This is the first things that
       is sent to the client after a query is received
       @param request The initial request
    */
    private void sendQueryId(RequestMessage request) throws IOException {
        ResponseMessage resp =
            new ResponseMessage(request, ResponseMessage.SERVER_QUERY_ID);
        sendResponse(resp);
    }

    /**Get a new query id
       @return new query id
    */
    private synchronized int getNextConnServerQueryId() {
        return (lastQueryId++);
    }

    public void sendErrMessage(RequestMessage reqMsg, int err_type,
            String message) {
        try {
            if (reqMsg == null) {
                // just do something stupid, anything, to get message back to
                // client if currentMesg is null, means error occured so early 
                // in parsing of the request message that localID and serverID 
                // could not be read
                reqMsg = new RequestMessage();
                reqMsg.localID = -1;
                reqMsg.serverID = -1;
            }
            ResponseMessage rm = new ResponseMessage(reqMsg, err_type);
            // add local id here in case padding is turned off !*#$*@$*
            rm.setData("SERVER ERROR - localID=\"" + reqMsg.localID
                    + "\" - Error Message: " + message);
            sendResponse(rm);
            closeConnection();
        } catch (IOException ioe) {
            System.err.println("\nERROR sending message \"" + message
                    + "\" to client " + "Error message: " + ioe.getMessage());
            ioe.printStackTrace();
        }
    }

    private void showCPUTimes() {
	ThreadMXBean mxbean = ManagementFactory.getThreadMXBean();
	long[] threadIDs = mxbean.getAllThreadIds();
	System.err.println("\tTHREADNAME\tCPUTIME");
	for (long threadID : threadIDs) {
	    long threadCPUTime = mxbean.getThreadCpuTime(threadID);
	    long diff = threadCPUTime;
	    if (threadCPUTimes.containsKey(threadID)) {
		diff -= threadCPUTimes.get(threadID);
	    } 
	    threadCPUTimes.put(threadID, threadCPUTime);
	    diff /= 1000000; // nanoseconds -> milliseconds
	    if (diff > 0)
		System.err.println("CPUTIME\t" + mxbean.getThreadInfo(threadID).getThreadName() + "\t" + diff);
	}
    }

    /**Class for storing the ServerQueryInfo objects into a hashtable and accessing it
       Essentially a wrapper around Hashtable class with similar functionality
    */
    class QueryList {
        Hashtable queryList;
        ArrayList removedQueryList;

        public QueryList() {
            queryList = new Hashtable();
            removedQueryList = new ArrayList();
        }

        public ServerQueryInfo get(int qid) {
            return (ServerQueryInfo) queryList.get(new Integer(qid));
        }

        public ServerQueryInfo put(int qid, ServerQueryInfo info) {
            return (ServerQueryInfo) queryList.put(new Integer(qid), info);
        }

        public ServerQueryInfo remove(int qid) {
            // System.out.println(
            //         "KT: Query with ServerQueryId "
            //         + qid
            //       + " removed from RequestHandler.QueryList "); 
            //return (ServerQueryInfo) queryList.remove(new Integer(qid));
            Integer temp = new Integer(qid);
            ServerQueryInfo removed = (ServerQueryInfo) queryList.remove(temp);
            removedQueryList.add(temp);
            return removed;
        }
        
        /*public void removeAll () {
        	removedQueryList.addAll(queryList.values());
        	queryList.clear();
        	
        }*/

        public boolean queryWasRun(int qid) {
            return removedQueryList.contains(new Integer(qid));
        }

        public Enumeration elements() {
            return queryList.elements();
        }
        
    }
}
