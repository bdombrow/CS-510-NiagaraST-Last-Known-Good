/*
 * $Id: HTTPRivuletServlet.java,v 1.1 2007/04/30 19:17:11 vpapad Exp $
 */

package niagara.connection_server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream; 
import java.io.OutputStreamWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HTTPRivuletServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final boolean doDebug = true;

    public void doPost(HttpServletRequest req, HttpServletResponse res)
            throws IOException {
        debug("Rivulet HTTP request received: " + req.getPathInfo());
        String method = req.getPathInfo();
        
        if (method.compareTo("/compile") == 0) {           
            String cql = req.getParameter("cqlsubmit");
            debug("cql=" + cql);
            res.setContentType("text/xml;boundary=\"<][>\"");
            this.doCGI("/usr/local/bin/compilecql", cql, res.getOutputStream());
        } else if (method.compareTo("/drawgraph") == 0) {
        	debug("draw graph method call received");
            String dot = req.getParameter("dot");
            res.setContentType("text/xml+svg;boundary=\"<][>\"");
            this.doCGI("dot -Tsvg", dot, res.getOutputStream());
        }
        
        debug("Rivulet HTTP request processed");
    }
    
    private void doCGI(String cmd, String input, OutputStream output) 
             throws IOException {
        Process proc = Runtime.getRuntime().exec(cmd);
        
        BufferedWriter childin = 
        	new BufferedWriter(new OutputStreamWriter(proc.getOutputStream()));
        BufferedReader childout = 
        	new BufferedReader(new InputStreamReader(proc.getInputStream()));

        debug("param = " + input);
        childin.write(input); 
        childin.close();
        
        BufferedWriter responsewriter =
            new BufferedWriter(new OutputStreamWriter(output));
        
        String line = childout.readLine();
        while (line != null) {
        	debug(line);
        	responsewriter.write(line);
        	line = childout.readLine();
        }
        responsewriter.close();
    }
    
    public String getServletInfo() {
        return "Rivulet HTTP servlet";
    }

    private static void cerr(String msg) {
        System.err.println("CS: " + msg);
    }

    private static final void debug(String msg) {
        if (doDebug) 
            cerr(msg);
    }
}

