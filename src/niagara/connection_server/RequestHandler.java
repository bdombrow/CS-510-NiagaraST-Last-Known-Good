/**********************************************************************
  $Id: RequestHandler.java,v 1.32 2003/12/24 02:16:38 vpapad Exp $


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
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.ArrayList;

import niagara.data_manager.ConstantOpThread;
import niagara.data_manager.StreamThread;
import niagara.optimizer.Optimizer;
import niagara.optimizer.Plan;
import niagara.optimizer.TracingOptimizer;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Op;
import niagara.physical.PhysicalAccumulate;
import niagara.query_engine.QueryResult;
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

    /**Constructor
       @param sock The socket to read from 
       @param server The server that has access to other modules
    */
    public RequestHandler(Socket sock, NiagraServer server)
        throws IOException {

        // A hashtable of queries with qid as key
        this.queryList = new QueryList();
        this.outputWriter =
            new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
        this.server = server;
        sendBeginDocument();

        this.xqpp = new XMLQueryPlanParser();
        this.optimizer = new Optimizer();
        // XXX vpapad: uncomment next line to get the tracing optimizer
        //this.optimizer = new TracingOptimizer();

        this.requestParser = new RequestParser(sock.getInputStream(), this);
        this.requestParser.startParsing();
    }

    // Send the initial string to the client
    private void sendBeginDocument() throws IOException {
        String header = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?>\n<response>\n";
        outputWriter.write(header);
        outputWriter.flush();
    }

    /**Handle the request that just came in from the client
       @param request The request that needs to be handled
     */
    public void handleRequest(RequestMessage request)
        throws
            InvalidPlanException,
            QueryResult.AlreadyReturningPartialException,
            ShutdownException,
            IOException {
        Plan plan, optimizedPlan;

        if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
            if (cpuTimer == null)
                cpuTimer = new CPUTimer();
            cpuTimer.start();
        }

        // Handle the request according to requestType
        switch (request.getIntRequestType()) {
            //   EXECUTE_QUERY request
            //-------------------------------------
            case RequestMessage.EXECUTE_QP_QUERY :
                plan = xqpp.parse(request.requestData);
                // Optimize the plan
                optimizedPlan = null;
                try {
                    optimizedPlan = optimizer.optimize(plan.toExpr());
                } catch (Exception e) {
                    System.err.println(
                        "XXX vpapad: exception occured during optimization");
                    e.printStackTrace();
                    assert false;
                }
                xqpp.clear();
                assignQueryId(request);
                processQPQuery(optimizedPlan, request);
                break;

            case RequestMessage.MQP_QUERY :
                plan = xqpp.parse(request.requestData);
                new MQPHandler(server.qe.getScheduler(), optimizer, plan);
                xqpp.clear();
                break;

            case RequestMessage.EXECUTE_QE_QUERY :
                // assign a new query id to this request
                int qid = getNextConnServerQueryId();

                // create and populate the query info
                ServerQueryInfo queryInfo =
                    new ServerQueryInfo(qid, ServerQueryInfo.QueryEngine);

                // start the transmitter thread for sending results back
                ResultTransmitter transmitter =
                    new ResultTransmitter(this, queryInfo, request);
                queryInfo.setTransmitter(transmitter);

                // now give the query to the query engine
                server.qe.executeQuery(transmitter, queryInfo, request.requestData);
                request.serverID = qid;
                sendQueryId(request);

                queryList.put(qid, queryInfo);
                break;

            case RequestMessage.SUSPEND_QUERY :
                throw new InvalidPlanException("Query suspension no longer allowed");

            case RequestMessage.RESUME_QUERY :
                throw new InvalidPlanException("Query suspension no longer allowed");

                //-------------------------------------
                //   KILL_QUERY request
                //-------------------------------------
            case RequestMessage.KILL_QUERY :
                killQuery(request.serverID);
                break;

                //-------------------------------------
                //   GET_NEXT request
                //-------------------------------------
            case RequestMessage.GET_NEXT :
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

                //-------------------------------------
                //   GET_PARTIAL request
                //-------------------------------------
            case RequestMessage.GET_PARTIAL :
                // Get the queryInfo object for this request
                queryInfo = queryList.get(request.serverID);

                // Respond to invalid queryID
                if (queryInfo == null)
                    assert false : "Bad query id " + request.serverID;

                // Put a get partial message upstream
                queryInfo.getQueryResult().requestPartialResult();
                break;

            case RequestMessage.RUN_GC :
                System.out.println("Starting Garbage Collection");
                long startime = System.currentTimeMillis();
                System.gc();
                long stoptime = System.currentTimeMillis();
                double executetime = (stoptime - startime) / 1000.0;
                System.out.println(
                    "Garbage Collection Completed."
                        + " Time: "
                        + executetime
                        + " seconds.");
                ResponseMessage doneMesg =
                    new ResponseMessage(request, ResponseMessage.END_RESULT);
                sendResponse(doneMesg);
                break;

            case RequestMessage.DUMPDATA :
                if (NiagraServer.RUNNING_NIPROF) {
                    System.out.println("Requesting profile data dump");
                    JProf.requestDataDump();
                    ResponseMessage doneDumpMesg =
                        new ResponseMessage(
                            request,
                            ResponseMessage.END_RESULT);
                    sendResponse(doneDumpMesg);
                } else {
                    System.out.println(
                        "Profiler not running - unable to dump data");
                    ResponseMessage errMesg =
                        new ResponseMessage(request, ResponseMessage.ERROR);
                    errMesg.setData(
                        "Profiler not running - unable to dump data");
                    sendResponse(errMesg);
                }
                break;

            case RequestMessage.SHUTDOWN :
                System.out.println("Shutdown message received");
                ResponseMessage shutMesg =
                    new ResponseMessage(request, ResponseMessage.END_RESULT);
                sendResponse(shutMesg);
                server.shutdown();
                break;
            case RequestMessage.SYNCHRONOUS_QP_QUERY :
                plan = xqpp.parse(request.requestData);

                // Optimize the plan
                optimizedPlan = null;
                try {
                    optimizedPlan = optimizer.optimize(plan.toExpr());
                } catch (Exception e) {
                    System.err.println("exception occured during optimization");
                    e.printStackTrace();
                }
                xqpp.clear();

                processQPQuery(optimizedPlan, request);
                // get the queryInfo of this query
                queryInfo = queryList.get(request.serverID);
                queryInfo.getTransmitter().handleSynchronousRequest();
                break;
            case RequestMessage.EXPLAIN_QP_QUERY :
                plan = xqpp.parse(request.requestData);

                // Optimize the plan
                optimizedPlan = null;
                try {
                    optimizedPlan = optimizer.optimize(plan.toExpr());
                } catch (Exception e) {
                    System.err.println("exception occured during optimization");
                    e.printStackTrace();
                }

                xqpp.clear();

                // We don't want to actually *run* the plan!
                // Replace the plan with a constant operator 
                // having the optimized plan as its content
                optimizedPlan =
                    new Plan(
                        new ConstantOpThread(
                            optimizedPlan.planToXML(),
                            new Attrs()));

                assignQueryId(request);
                processQPQuery(optimizedPlan, request);

                break;

                //-------------------------------------
                //   Ooops 
                //-------------------------------------
            default :
                throw new PEException(
                    "ConnectionThread: INVALID_REQUEST "
                        + request.getIntRequestType());
        }

        if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
            cpuTimer.stop();
            cpuTimer.print("HandleRequest (" + request.requestType + ")");
        }

    }

    private void assignQueryId(RequestMessage request) throws IOException {
        request.serverID = getNextConnServerQueryId();
        sendQueryId(request);
    }

    private void processQPQuery(Plan plan, RequestMessage request)
        throws InvalidPlanException, ShutdownException, IOException {
        // XXX vpapad: commenting out code is a horrible sin
        //            if (type.equals("submit_subplan")) {
        //                //                // The top operator better be a send...
        //                //                // XXX vpapad: Argh... Plan or logNode?
        //                //                SendOp send = (SendOp) ((logNode) top).getOperator();
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
            (request.getIntRequestType()
                == RequestMessage.SYNCHRONOUS_QP_QUERY);

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
    }

    /**Method used by everyone to send responses to the client
       @param mesg The message that needs to be sent
    */
    public synchronized void sendResponse(ResponseMessage mesg)
        throws IOException {
        ServerQueryInfo sqi = queryList.get(mesg.getServerID());
        boolean padding = true; // is this the correct default?
        if (sqi != null) {
            padding = !sqi.isSynchronous();
        }
        mesg.toXML(outputWriter, padding);
        mesg.clearData();
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

        // Process Kill message
        // Remove the query from the active queries list
        queryList.remove(queryID);

        // destroy the transmitter thread
        assert queryInfo.getTransmitter()
            != null : "KT way bad transmitter is null";
        queryInfo.getTransmitter().destroy();

        // Put a KILL control message down stream
        queryInfo.getQueryResult().kill();

        if (queryInfo.isSynchronous()) {
            try {
                outputWriter.close();
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
            // 	       "KT: Query with ServerQueryId "
            //	       + qid
            //       + " removed from RequestHandler.QueryList "); 
            //return (ServerQueryInfo) queryList.remove(new Integer(qid));
            Integer temp = new Integer(qid);
            ServerQueryInfo removed = (ServerQueryInfo) queryList.remove(temp);
            removedQueryList.add(temp);
            return removed;
        }

        public boolean queryWasRun(int qid) {
            return removedQueryList.contains(new Integer(qid));
        }

        public Enumeration elements() {
            return queryList.elements();
        }
    }
}
