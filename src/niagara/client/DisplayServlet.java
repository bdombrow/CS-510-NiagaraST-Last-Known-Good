package niagara.client;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;

public class DisplayServlet extends HttpServlet {

    private static boolean doDebug = false;

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {
            debug("handling POST request");
            BufferedReader in = new BufferedReader(new InputStreamReader(
                req.getInputStream()));
            if (!MQPClient.quiet)
                System.out.println("<results query_id='" + in.readLine() + "'>");
            String input = null;
            while ((input = in.readLine()) != null) {
                if (!MQPClient.quiet)
                    System.out.println(input);
            }
            if (!MQPClient.quiet)
                System.out.println("</results>");
            res.getOutputStream().close();
            System.out.println("Query done: " + (new Date()).getTime() % (60 * 60 * 1000));
            
            debug("request handled");
            System.exit(0);
        }
        catch (Exception e) {
            cerr("An exception occured:");
            e.printStackTrace();
        }
    }

    public static void cerr(String msg) {
        System.err.println("SCS: " + msg);
    }

    public static void debug(String msg) {
        if (doDebug) 
            System.err.println("DisplayServlet: " + msg);
    }
}
