/**
 * $Id: DisplayServlet.java,v 1.5 2003/07/08 02:10:37 tufte Exp $
 */

package niagara.client;

import javax.servlet.http.*;
import java.io.*;


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
