/**
 * $Id: PhysicalDisplay.java,v 1.1 2003/12/24 01:49:01 vpapad Exp $
 *
 */

package niagara.physical;

import org.w3c.dom.*;

import niagara.optimizer.colombia.*;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.logical.*;
import niagara.ndom.DOMFactory;

import java.io.*;
import java.net.*;

/**
 * Implementation of Display. 
 */

public class PhysicalDisplay extends PhysicalOperator {
    // No blocking inputs
    private static final boolean[] blockingSourceStreams = { false };

    private PrintWriter pw;
    private OutputStream out;

    private StringBuffer toDisplay;

    private String query_id;

	private Element nullElt;

    String url_location;

    URLConnection connection;

    public PhysicalDisplay() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    protected PhysicalDisplay(String query_id, String url_location) {
        this();
        this.query_id = query_id;
        this.url_location = url_location;        
                
        toDisplay = new StringBuffer();
    }
    
    public Op opCopy() {
        return new PhysicalDisplay(query_id, url_location);
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
        Display display = (Display) logicalOperator;
        query_id = display.getQueryId();
        url_location = display.getClientLocation();
        toDisplay = new StringBuffer();
    }
    
    public void opInitialize() {
    	Document doc = DOMFactory.newDocument();
    	nullElt = doc.createElement("niagara:null");
    }
    
    public int hashCode() {
        return query_id.hashCode() ^ url_location.hashCode();
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalDisplay))
            return false;
        if (o.getClass() != PhysicalDisplay.class)
            return o.equals(this);
        PhysicalDisplay other = (PhysicalDisplay) o;
        return query_id.equals(other.query_id) && url_location.equals(other.url_location);
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
    protected void processTuple(
        Tuple tupleElement,
        int streamId)
        throws ShutdownException {
        if (counter == 0) {
            try {
                URL url = new URL(url_location);
                connection = url.openConnection();
                connection.setDoOutput(true);
                out = connection.getOutputStream();
                pw = new PrintWriter(out);
                pw.println(query_id);
            } catch (java.net.MalformedURLException mue) {
                throw new ShutdownException(
                    "Bad url for client " + mue.getMessage());
            } catch (java.io.IOException ioe) {
                throw new ShutdownException(
                    "Unable to open connection to client "
                        + url_location
                        + " "
                        + ioe.getMessage());
            }
        }

        counter++;

        // We assume result is the last element of the tuple...
        Object attribute = tupleElement.getAttribute(tupleElement.size() - 1);
    
    	if(attribute == null)
    		attribute = nullElt;    
         
        if (attribute instanceof Document) {
            // Serialize its root element instead
            attribute = ((Document) attribute).getDocumentElement();
        }
        serialize(attribute, toDisplay);
        pw.println(toDisplay.toString());
        toDisplay.setLength(0);
    }

    protected void serialize(Object o, StringBuffer sb) {
        XMLUtils.flatten((Node) o, sb, false, false);
    }

    protected void cleanUp() {
        try {
            pw.flush();
            out.close();
            connection.getInputStream().close();
        } catch (java.io.IOException e) {
            System.err.println(
                "Display: error while sending results to client:"
                    + url_location);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public boolean isStateful() {
        // XXX vpapad: Display *is* stateful, but does not know how to 
        // handle partial results...
        return false;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        // Setup costs for opening a connection, plus a fixed cost per transmitted tuple
        Cost final_cost = new Cost(catalog.getDouble("open_connection_cost"));
        Cost tuple_transmission = new Cost(catalog.getDouble("tuple_transmission_cost"));
        final_cost.add(tuple_transmission.times(InputLogProp[0].getCardinality()));
        return final_cost;
    }
    
    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;
        outputTupleSchema = new TupleSchema();
    }
    
    /**
     * @see niagara.utils.SerializableToXML#dumpAttributesInXML(StringBuffer)
     */
    public void dumpAttributesInXML(StringBuffer sb) {
        sb.append(" query_id='").append(query_id).append("' location='");
        sb.append(url_location).append("'");     
    }

}
