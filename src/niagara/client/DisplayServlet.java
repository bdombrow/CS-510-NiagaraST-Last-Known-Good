/**
 * $Id: DisplayServlet.java,v 1.4 2002/10/29 01:56:22 vpapad Exp $
 */

package niagara.client;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;

public class DisplayServlet extends HttpServlet {

    private static boolean doDebug = false;
    public void doPost(HttpServletRequest req, HttpServletResponse res) 
        throws IOException {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                                                       req.getInputStream()));
            if (!MQPClient.quiet)
                System.out.println("<niagara:results query_id='" + 
                                   in.readLine() + "'>");
            String input = null;
            while ((input = in.readLine()) != null) {
                if (!MQPClient.quiet)
                    System.out.println(input);
            }
            if (!MQPClient.quiet)
                System.out.println("</niagara:results>");
            res.getOutputStream().close();
            MQPClient.getMQPClient().queryDone();            
    }
}
