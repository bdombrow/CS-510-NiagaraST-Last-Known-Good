/*
 * $Id: CommunicationServlet.java,v 1.5 2002/10/31 04:20:30 vpapad Exp $
 */

package niagara.connection_server;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.*;

import niagara.query_engine.*;
import niagara.data_manager.ConstantOpThread;
import niagara.optimizer.Optimizer;
import niagara.optimizer.Plan;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Expr;
import niagara.xmlql_parser.op_tree.*;
import niagara.utils.*;

public class CommunicationServlet extends HttpServlet {
    private NiagraServer server;
    private Optimizer optimizer;
    private int id;

    // maps subquery id -> Send operator
    private Hashtable queries;

    private static final boolean doDebug = true;

    private XMLQueryPlanParser xqpp;

    private synchronized String nextId() {
        return String.valueOf(id++);
    }

    public void init(ServletConfig sc) throws ServletException {
        server = (NiagraServer) sc.getServletContext().getAttribute("server");

        id = 0;
        queries = new Hashtable();
        xqpp = new XMLQueryPlanParser();
        optimizer = new Optimizer();
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

    public void setSend(String query_string, PhysicalSendOperator send) {
        queries.put(query_string, send);
    }

   private void handleDistributedPlans(ArrayList remotePlans) {
    //XXX vpapad: commenting out code is a horrible sin
//        for (int i = 0; i < remotePlans.size(); i++) {
//            // The top operator of every remote plan should be a SendOp
//            SendOp send = (SendOp) ((Expr) remotePlans.get(i)).getOp();
//            send.setToLocation(NiagraServer.getLocation());
//            String url_location =
//                "http://" + send.getFromLocation() + "/servlet/communication";
//
//            String query_id = "";
//
//            try {
//                String encodedQuery = URLEncoder.encode(node.subplanToXML());
//
//                //System.err.println("ES: Getting subplan id...");
//                URL url = new URL(url_location);
//                URLConnection connection = url.openConnection();
//                connection.setDoOutput(true);
//                PrintWriter out = new PrintWriter(connection.getOutputStream());
//                out.println("type=submit_subplan&query=" + encodedQuery);
//                out.flush();
//                out.close();
//
//                BufferedReader in =
//                    new BufferedReader(
//                        new InputStreamReader(connection.getInputStream()));
//                query_id = in.readLine();
//                in.close();
//
//                // Replace the subplan with a ReceiveOp
//                ReceiveOp recv = new ReceiveOp();
//                recv.setReceive(location, query_id);
//                logNode rn = new logNode(recv);
//                scheduleForExecution(
//                    rn,
//                    outputStream,
//                    nodesScheduled,
//                    doc,
//                    null);
//                return;
//            } catch (MalformedURLException mue) {
//                System.err.println(
//                    "Malformed URL "
//                        + mue.getMessage()
//                        + " url "
//                        + url_location);
//                mue.printStackTrace();
//            } catch (IOException ioe) {
//                System.err.println(
//                    "Execution scheduler: io exception " + ioe.getMessage());
//                ioe.printStackTrace();
//            }
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
