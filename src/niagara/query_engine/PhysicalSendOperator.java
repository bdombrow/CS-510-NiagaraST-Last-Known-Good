/**
 * $Id: PhysicalSendOperator.java,v 1.1 2001/07/17 07:03:47 vpapad Exp $
 *
 */

package niagara.query_engine;

import java.util.*;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.ndom.*;
import niagara.connection_server.*;

import java.io.*;

/**
 * This is the <code>PhysicalSendOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Send
 * operator.
 *
 * @version 1.0
 *
 */

public class PhysicalSendOperator extends PhysicalOperator {
    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // We will use this Document as the "owner" of all the DOM nodes
    // we create
    private Document txd;

    private CommunicationServlet cs;

    private OutputStream outputStream;
    private PrintWriter pw;

    private StringBuffer toSend;

    private String query_id;

    private StreamMaterializer streamMaterializer;

    /**
     * This is the constructor for the PhysicalSendOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * destination streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */

    public PhysicalSendOperator(op logicalOperator,
				     Stream[] sourceStreams,
				     Stream[] destinationStreams,
				     Integer responsiveness) {


	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);
        
        SendOp send = (SendOp) logicalOperator;
        query_id = send.getQueryId();
        cs = send.getCS();
        cs.setSend(query_id, this);

	txd = DOMFactory.newDocument();

        toSend = new StringBuffer();

        streamMaterializer = new StreamMaterializer();
    }
    

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a non-blocking state. This over-rides the
     * corresponding function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return True if the operator is to continue and false otherwise
     */


    int counter; // XXX
    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement tupleElement,
						 int streamId,
						 ResultTuples result) {
        counter++;

        // block until connection is established
        while (pw == null) {
            System.err.println("XXX waiting for connection...");
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException ie) {
                ;
            }
        }

        boolean useStreamMaterializer = true;
        
        
        if (!useStreamMaterializer) {
            toSend.setLength(0);
            toSend.append("<tuple>");
            // For every thing in the tuple...
            for (int i = 0; i < tupleElement.size(); i++) {
                toSend.append("<elt>");
                
                Object attribute = tupleElement.getAttribute(i);
                if (attribute instanceof Document) {
                    // Serialize its root element instead
                attribute = ((Document) attribute).getDocumentElement();
                }
                serialize(attribute, toSend);
                
                toSend.append("</elt>");
            }
        
            toSend.append("</tuple>");
            // XXX send buffer
            pw.println(toSend.toString());
        }
        else { // Using stream Materializer
            streamMaterializer.process(tupleElement);
            pw.println(streamMaterializer.getOutput());
        }
        // Signal end of tuple
        pw.println("\0");
        pw.flush();
	// The operator can now continue
	//
	return true;
    }

    protected void serialize(Object o, StringBuffer sb) {
        // XXX we don't handle attributes
        if (o == null)
            return;

        if (o instanceof Text) 
            sb.append(((Text) o).getData());

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
        pw.println("\0\0");
        pw.flush();
        
        try {
            outputStream.close();
        }
        catch (IOException ioe) {
            ;
        }
        Date d = new Date();
        System.out.println("Query done: " + d.getTime() % (60 * 60 * 1000));

        cs.queryDone(query_id);
    }
    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        pw = new PrintWriter(outputStream);
    }
}

