/**
 * $Id: PhysicalDisplayOperator.java,v 1.2 2002/04/29 19:51:23 tufte Exp $
 *
 */

package niagara.query_engine;

import java.util.ArrayList;
import java.util.Vector;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * This is the <code>PhysicalDisplayOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Display
 * operator.
 *
 * @version 1.0
 *
 */

public class PhysicalDisplayOperator extends PhysicalOperator {
    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    private PrintWriter pw;
    private OutputStream out;

    private StringBuffer toDisplay;

    private String query_id;

    String url_location;

    URLConnection connection;

    /**
     * This is the constructor for the PhysicalDisplayOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * sink streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */

    public PhysicalDisplayOperator(op logicalOperator,
				   SourceTupleStream[] sourceStreams,
				   SinkTupleStream[] sinkStreams,
				   Integer responsiveness) {


	// Call the constructor of the super class
	//
	super(sourceStreams,
	      sinkStreams,
	      blockingSourceStreams,
	      responsiveness);
        
        DisplayOp display = (DisplayOp) logicalOperator;
        query_id = display.getQueryId();
        String location = display.getClientLocation();

        toDisplay = new StringBuffer();
        url_location = "http://" + location + "/servlet/display";
    }
    

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a non-blocking state. This over-rides the
     * corresponding function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */


    int counter = 0;
    protected void nonblockingProcessSourceTupleElement (
		     			 StreamTupleElement tupleElement,
						 int streamId) 
	throws ShutdownException{
        if (counter == 0) {
            try {
                URL url = new URL(url_location);
                connection = url.openConnection();
                connection.setDoOutput(true);
                out = connection.getOutputStream();
                pw = new PrintWriter(out);
                pw.println(query_id);
            }
            catch (Exception e) {
                System.out.println("Could not connect to client: " + url_location);
                e.printStackTrace();
            }
        }

        counter++;


        // We assume result is the last element of the tuple...
        Object attribute = tupleElement.getAttribute(
            tupleElement.size()-1);
        if (attribute instanceof Document) {
            // Serialize its root element instead
            attribute = ((Document) attribute).getDocumentElement();
        }
        serialize(attribute, toDisplay);
        pw.println(toDisplay.toString());
        toDisplay.setLength(0);
    }

    protected void serialize(Object o, StringBuffer sb) {
        // XXX we don't handle attributes
        if (o == null)
            return;

        if (o instanceof Text) {
            sb.append(((Text) o).getData());
        }

        else if (o instanceof Element) {
            sb.append("<" + ((Element) o).getTagName() + ">");
            NodeList nl = ((Element) o).getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                serialize(nl.item(i), sb);
            }
            sb.append("</" + ((Element) o).getTagName() + ">");
        }
        else {
            System.out.println("XXX ignoring attribute of type: " + o.getClass());
        }
    }

    protected void cleanUp () {
          try {
              System.out.println("XXX display transmitted " + counter  + " tuples");
              pw.flush();
              out.close();
              connection.getInputStream().close();
              Date d = new Date();
              System.out.println("Query done: " + d.getTime() % (60 * 60 * 1000));
          }
          catch (Exception e) {
              System.out.println("Display: error while sending results to client:" + url_location);
              e.printStackTrace();
              System.exit(-1);
          }
    }

    public boolean isStateful() {
	return false;
    }
}

