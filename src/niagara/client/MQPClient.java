/**
 * $Id: MQPClient.java,v 1.8 2003/09/22 01:16:01 vpapad Exp $
 */

package niagara.client;

import niagara.utils.XMLUtils;

import org.apache.xerces.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;

import java.io.StringReader;

// For Jetty Servlet engine
import com.mortbay.HTTP.*;
import com.mortbay.Util.*;
import com.mortbay.HTTP.Handler.Servlet.*;

public class MQPClient extends SimpleClient {
    // ??? still requires xerces parser - do we want
    // to convert this to niagara.ndom.DOMParser??
    private static DOMParser parser;
    
    private static MQPClient mqpclient;
    
    private static int queryId;
    private static String nextId() {
        return String.valueOf(++queryId);
    }
    
    public MQPClient(String host, int port) {
        SimpleConnectionReader cr = new SimpleConnectionReader(host, port, this);
        // Do not proceed if we didn't manage to connect
        if (!cr.isInitialized()) 
            return;
        cm = new ConnectionManager(cr);
        mqpclient = this;
    }

    public static MQPClient getMQPClient() {
        return mqpclient;
    }
    
    public void processQuery(String queryText) {
        // parse the query string
        InputSource is = new InputSource(new StringReader(queryText));

        try {
            parser.parse(is);
        } catch (Exception e) {
            cerr("Exception while parsing plan file: ");
            e.printStackTrace();
            System.exit(-1);
        }

        Element plan = null;
        String query = "";

        Document d = parser.getDocument();

        // Get root element
        plan = d.getDocumentElement();

        // Change 'top' attribute to 'display'
        String old_top = plan.getAttribute("top");
        plan.setAttribute("top", "display");

        // Create a display element and set id and client attributes
        Element display = d.createElement("display");
        display.setAttribute("id", "display");
        display.setAttribute("input", old_top);
        StringBuffer loc = new StringBuffer();
        loc.append("http://").append(clientHost).append(":").
            append(clientPort).append("/servlet/display");
        display.setAttribute("client_location", loc.toString());
        display.setAttribute("query_id", nextId());

        // Add display as the first child of plan
        plan.insertBefore(display, plan.getFirstChild());

        // Get the query in string format
	// don't use prettyprint - vassilis is a unix guy, he certainly
	// won't want any extra characters messing up his query
        // XXX vassilis: of course not!
        query = XMLUtils.flatten(d, false); 

        // Send plan to server, and wait for results
        m_start = System.currentTimeMillis();
        try {
            cm.executeQuery(new MQPQuery(query),
                            Integer.MAX_VALUE);
        } catch (ClientException ce) {
            errorMessage(queryId, ce.getMessage());           
        }
    }

    public static void main(String args[]) {
        parser = new DOMParser();        
        parseArguments(args);
        try {
            parser.setFeature("http://xml.org/sax/features/validation", false);
            parser.setFeature(
                "http://apache.org/xml/features/nonvalidating/load-external-dtd",
                false);

        } catch (SAXException e) {
            System.out.println("error in setting up parser feature");
            e.printStackTrace();
        }
        
        MQPClient c = new MQPClient(host, port);
        c.startHTTPServer();
        c.processQueries();
    }

    void startHTTPServer() {
        try {
            // Start HTTP server for interserver communication
            HttpServer hs = new HttpServer();
            hs.addListener(new InetAddrPort(clientPort));
            HandlerContext hc = hs.addContext(null, "/servlet/*");

            ServletHandler sh = new ServletHandler();
            ServletHolder sholder =
                sh.addServlet("/display", "niagara.client.DisplayServlet");

            hc.addHandler(sh);
            hs.start();
        } catch (Exception e) {
            cerr("Exception while setting up HTTP server:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void cerr(String msg) {
        System.err.println("MQPClient: " + msg);
    }
}
