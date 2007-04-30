/*
 * $Id: HTTPClientServlet.java,v 1.1 2007/04/30 19:17:10 vpapad Exp $
 */

package niagara.connection_server;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HTTPClientServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private NiagraServer server;

    private static final boolean doDebug = true;

    
    public void init(ServletConfig sc) throws ServletException {
        server = (NiagraServer) sc.getServletContext().getAttribute("server");
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res)
            throws IOException {
        debug("HTTP request received");
        res.setContentType("multipart/x-mixed-replace;boundary=\"<][>\"");
        cerr("query= " + req.getParameter("query"));
        new RequestHandler(req.getParameter("query"), req.getParameter("type"),
                res, server);
        debug("HTTP request processed");
    }
    
    public String getServletInfo() {
        return "Niagara HTTP client servlet";
    }

    private static void cerr(String msg) {
        System.err.println("CS: " + msg);
    }

    private static final void debug(String msg) {
        if (doDebug) 
            cerr(msg);
    }
}
