/*
 * $Id: CommunicationServlet.java,v 1.4 2002/10/12 20:11:06 tufte Exp $
 *
 */

package niagara.connection_server;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;

import niagara.query_engine.*;
import niagara.optimizer.Optimizer;
import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

public class CommunicationServlet extends HttpServlet {
    private NiagraServer server;

    private int id;

    // maps subquery id -> Send operator
    private Hashtable queries;

    private static final boolean doDebug = true;

    private XMLQueryPlanParser xqpp;

    private synchronized String nextId() {
        return "" + id++;
    }

    public void init(ServletConfig sc) throws ServletException {
        server = (NiagraServer) sc.getServletContext().getAttribute("server");

        id = 0;
        queries = new Hashtable();
        xqpp = new XMLQueryPlanParser();
    }

    public void doGet(HttpServletRequest req, HttpServletResponse res) 
	throws IOException {
        try {
            String type = req.getParameter("type");
            debug("handling GET request");
            if (type.equals("get_tuples")) {
                String query_id = req.getParameter("id");
                if (!queries.containsKey(query_id))
                    cerr("Request for an unknown query id!");
                
                while (true) {
                    Object o = queries.get(query_id);
                    if (!(o instanceof PhysicalSendOperator)) {
                        // Send operator not running yet
                        Thread.sleep(200);
                    }
                    else {
                        PhysicalSendOperator send = (PhysicalSendOperator) o;
                        send.setOutputStream(res.getOutputStream());
                        break;
                    }
                }
            }

            debug("request handled");

	} catch (InterruptedException e) {
	    throw new UnrecoverableException("Interrupted exception - what should I do???? HELP - KT");
        }
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) 
        throws IOException {
        try {
            Date d = new Date();
            System.out.println("Query Received: " + d.getTime() % (60 * 60 * 1000));

            debug("handling POST request");
            InputStream is = req.getInputStream();
            String type = req.getParameter("type");
            String query = req.getParameter("query");
            is.close();



            ServletOutputStream out = res.getOutputStream(); 
            logNode top;
            
            xqpp.initialize();
            
            // Try to parse the query plan
            try {
                top = xqpp.parse(query);
            }
            catch (XMLQueryPlanParser.InvalidPlanException ipe) {
                cerr("error while parsing plan");                    
                ipe.printStackTrace();
                out.println("-1");
                out.close();
                return;
            } 

            top = Optimizer.optimize(top);

            if (type.equals("submit_subplan")) {
                // The top operator better be a send...
                SendOp send = (SendOp) top.getOperator();

                String query_id = nextId();
                send.setQueryId(query_id);
                send.setCS(this);
                send.setSelectedAlgoIndex(0);

                queries.put(query_id, send);
                
                out.println(query_id);
                out.close();
            } else if (type.equals("submit_plan")) {
                out.println("Query received");
                out.close();
            } else if (type.equals("optimize_plan")) {
                debug("plan optimization requested");
                out.println("Query received");
                out.close();

                // We don't want to actually *run* the plan!
                // Replace the plan with a constant operator 
                // having the optimized plan as its content
                ConstantOp op = new ConstantOp();
                op.setContent(top.input().planToXML());
                op.setSelectedAlgoIndex(0);
                top.setInput(new logNode(op), 0);
            } else { 
                throw new PEException("Client request with unknown type: " 
                                      + type);
            }

            debug("Sending to ES");

            ExecutionScheduler es = server.getQueryEngine().getScheduler();
            es.scheduleSubPlan(top);

            debug("request handled");
	}
	catch (ShutdownException e) {
	    cerr("ShutdownException occurred during doPost:");
	    e.printStackTrace();
	}
    }

    public void setSend(String query_string, PhysicalSendOperator send) {
        queries.put(query_string, send);
    }

    public void queryDone(String query_id) {
        queries.remove(query_id);
    }

    public String getServletInfo() {
        return "Niagara inter-server communication servlet";
    }

    public static void cerr(String msg) {
        System.err.println("CS: " + msg);
    }

    public static final void debug(String msg) {
        if (doDebug) 
            System.err.println("CS: " + msg);
    }
}
