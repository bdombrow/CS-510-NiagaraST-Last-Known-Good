/**
 * $Id: PhysicalSend.java,v 1.1 2003/12/24 01:49:04 vpapad Exp $
 *
 */

package niagara.physical;

import java.util.*;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.logical.*;
import niagara.ndom.*;
import niagara.optimizer.colombia.LogicalOp;
import niagara.connection_server.*;

import java.io.*;

/**
 * This is the <code>PhysicalSendOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Send
 * operator.
 */
// XXX vpapad: making this unoptimizable for the time being
// to get CVS to compile
public class PhysicalSend extends UnoptimizablePhysicalOperator {
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

    public PhysicalSend() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
        Send send = (Send) logicalOperator;
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
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    int counter; // XXX
    protected void processTuple (
				 Tuple tupleElement,
				 int streamId)
	throws ShutdownException, InterruptedException{
        counter++;

        // block until connection is established
        while (pw == null) {
            System.err.println("XXX waiting for connection...");
	    Thread.sleep(200);
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

    public boolean isStateful() {
	return false;
    }
}

