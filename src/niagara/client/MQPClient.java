package niagara.client;

import niagara.utils.XMLUtils;

import org.apache.xerces.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;

import java.io.*;
import java.net.*;
import java.util.*;

// For Jetty Servlet engine
import com.mortbay.HTTP.*;
import com.mortbay.Util.*;
import com.mortbay.HTTP.Handler.*;
import com.mortbay.HTTP.Handler.Servlet.*;

public class MQPClient {
    private String client_host;
    private int client_port;
    private String server_host;
    private int server_port;
    private String qfname;

    static boolean serverUp;

    static boolean quiet = false;

    private int query_id = 0;

    private String nextId() {
        return String.valueOf(++query_id);
    }

    public MQPClient(String client_host, int client_port, String server_host, int server_port, String qfname) {
        this.client_host = client_host;
        this.client_port = client_port;
        this.server_host = server_host;
        this.server_port = server_port;
        this.qfname = qfname;
    }

    public void run() {
        // parse the query file
	// ??? still requires xerces parser - do we want
	// to convert this to niagara.ndom.DOMParser??
        DOMParser parser = new DOMParser();
        try {
            parser.setFeature("http://xml.org/sax/features/validation", false);
            parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

        } catch (SAXException e) {
            System.out.println("error in setting up parser feature");
            e.printStackTrace();
        }


        InputSource is = null;
        try {
            is = new InputSource(new FileInputStream(qfname));
        }
        catch (Exception e) {
            cerr("Could not open file: " + qfname);
            System.exit(-1);
        }

        try {
            parser.parse(is);
        }
        catch (Exception e) {
            cerr("Exception while parsing plan file: ");
            e.printStackTrace();
            System.exit(-1);
        }


        Element plan = null;
        String query = "";
        try {
            // Get document
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
            String client_location = client_host + ":" + client_port;
            display.setAttribute("client_location", client_location);
            display.setAttribute("query_id", nextId());

            // Add display as the first child of plan
            plan.insertBefore(display, plan.getFirstChild());

            // Get the query in string format
            query = XMLUtils.flatten(plan);
        }
        catch (Exception e) {
            cerr("Exception while building plan:");
            e.printStackTrace();
            System.exit(-1);
        }

        startHTTPServer();

        // Send plan to server, and wait for results
        String url_location = "http://" + server_host 
            + ":" + server_port + "/servlet/communication";
        try {
            String encodedQuery = URLEncoder.encode(query);

            System.out.println("Sending query: " + (new Date()).getTime() % (60 * 60 * 1000));
            URL url = new URL(url_location);
            URLConnection connection = url.openConnection();
            connection.setDoOutput(true);
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.println("type=submit_plan&query=" + encodedQuery);
            out.close();
            
            connection.getInputStream().close();
        }
          catch (Exception e) {
              System.out.println("Exception while sending query to server:");
              e.printStackTrace();
              System.exit(-1);
          }
    }

    public static void main(String args[]) {
        int client_port = -1, server_port = -1;
        String client_host= null, server_host = null, qfname = null;

        int opt = 0;
        if (args[0].equals("-q")) {
            quiet = true;
            opt = 1;
        }
	if ((args.length - opt) != 5) {
	    cerr("Usage: MQPClient [-q] <client_host> <client port> <server host>" +  
                 " <server port> <query file name>");
	    System.exit(-1);
	}
        

        try {
            client_host = args[opt];
            client_port = Integer.parseInt(args[opt+1]);
            server_host = args[opt+2];
            server_port = Integer.parseInt(args[opt+3]);
            qfname = args[opt+4];
        }
        catch (Exception e) {
            cerr("Error parsing arguments.");
        }

       MQPClient c = new MQPClient(client_host, client_port, server_host, server_port, qfname);
       c.run();
    }

    void startHTTPServer() {
        try {
            // Start HTTP server for interserver communication
            HttpServer hs = new HttpServer();
            hs.addListener(new InetAddrPort(client_port));
            HandlerContext hc = hs.addContext(null, "/servlet/*");
            
            ServletHandler sh = new ServletHandler();
            ServletHolder sholder = sh.addServlet("/display",
                                                  "niagara.client.DisplayServlet");
                
            hc.addHandler(sh);
            hs.start();
            }
        catch (Exception e) {
            cerr("Exception while setting up HTTP server:");
            e.printStackTrace();
            System.exit(-1);
        }
            MQPClient.serverUp = true;
    }

    public static void cerr(String msg) {
        System.err.println("MQPClient: " + msg);
    }
}
