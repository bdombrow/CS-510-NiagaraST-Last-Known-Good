/**
 * $Id: ConstantOpThread.java,v 1.10 2003/12/24 02:12:09 vpapad Exp $
 *
 */

package niagara.data_manager;

/** Niagara DataManager
  * ConstantOpThread - retrieve data from an embedded document and
  * put it into the stream; based on FirehoseThread
  */

import org.w3c.dom.*;
import java.io.*;
import java.util.*;

import org.xml.sax.*;

import niagara.query_engine.*;
import niagara.utils.*;
import niagara.logical.ConstantScan;
import niagara.ndom.*;
import niagara.optimizer.colombia.*;

public class ConstantOpThread extends SourceThread {
    // Optimization-time attributes
    private Attrs vars;
    
    private SinkTupleStream outputStream;

    private String content;

    public ConstantOpThread() {};
    
    public ConstantOpThread(String content, Attrs vars) {
	this.content = content;
        this.vars = vars;
    }

    public void plugIn(SinkTupleStream outputStream, DataManager dm) {
        this.outputStream = outputStream;
    }
    
    /**
     * Thread run method
     *
     */
    public void run() {
	// Our work is finished
	try {
            processDocument();
	    outputStream.endOfStream();
	} catch (SAXException e) {
	    // not sure what should be done here, just throw this error 
	    // for now, someone should fix, this KT
	    throw new PEException("Error processing document in ConstantOpThread ");
	} catch (InterruptedException ie) {
	    // same here
	    System.out.println("IOError closing stream in ConstantOpThread " +
			       ie.getMessage());
	    ie.printStackTrace();
	} catch (ShutdownException se) {
	    // same here
	    System.out.println("IOError closing stream in ConstantOpThread " +
			       se.getMessage());
	    se.printStackTrace();
	} catch(java.io.IOException ioe) {
	    // same here
	    System.out.println("IOError closing stream in ConstantOpThread " +
			       ioe.getMessage());
	    ioe.printStackTrace();
	}
    }

    /** process the incoming message which was just put in the message
     * buffer - this means parse it and put the resulting Document
     * in the output stream
     */
    private void processDocument() 
	throws SAXException, ShutdownException, IOException {
	try {
	    niagara.ndom.DOMParser parser;
	    if (niagara.connection_server.NiagraServer.usingSAXDOM()) 
		parser = DOMFactory.newParser("saxdom");
	    else
		parser = DOMFactory.newParser();

	    parser.parse(new InputSource(new ByteArrayInputStream(content.getBytes())));
	    Document doc = parser.getDocument();

            // XXX vpapad: change this to niagara:tuplestream or something
            // like that        
            if (doc.getDocumentElement().getTagName().equals("stream")) {
                Hashtable elements = new Hashtable();
                processStreamDoc(doc.getDocumentElement(), elements);
                
                NodeList tuples = doc.getDocumentElement().getElementsByTagName("tuple");
                for (int i = 0; i < tuples.getLength(); i++) {
                    Element tuple = (Element) tuples.item(i);
                    NodeList attrs = tuple.getElementsByTagName("elt");
                    Tuple ste = new Tuple(false);
                    for (int j = 0; j < attrs.getLength(); j++) {
                        ste.appendAttribute(attrs.item(j).getFirstChild());
                    }
                    outputStream.putTupleNoCtrlMsg(ste);
                }
            }
            else // A regular costant doc.
		outputStream.put(doc);

	} catch(java.lang.InterruptedException e) {
	    System.err.println("Thread interrupted in ConstantOpThread::processMessageBuffer");
	}
	return;
    }

    void processStreamDoc(Element root, Hashtable elements) {
        Stack toCheck = new Stack();
        toCheck.push(root);
        
        while(!toCheck.isEmpty()) {
            Element e = (Element) toCheck.pop();
            
            NodeList nl = e.getChildNodes();
            for (int i = nl.getLength() - 1; i >= 0; i--) {
                if (nl.item(i) instanceof Element) {
                    Element c = (Element) nl.item(i);
                    if (!c.getTagName().equals("eltref")) {
                        toCheck.add(c);
                        
                        if (!c.getAttribute("eid").equals("")) {
                            
                            elements.put(c.getAttribute("eid"), c);
                        }
                    }
                    else {
                        e.replaceChild((Node) elements.get(
                            c.getAttribute("eid")), c);
                    }
                }
            }
        }
    }
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        return new Cost(catalog.getDouble("document_parsing_cost") +
                        1*catalog.getDouble("tuple_construction_cost"));
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
     */
    public void opInitFrom(LogicalOp op) {
        ConstantScan cop = (ConstantScan) op;
        this.vars = cop.getVars();
        this.content = cop.getContent();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        return new ConstantOpThread(content, vars);
    }

    public int hashCode() {
        return content.hashCode() ^ vars.hashCode();
    }
    
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ConstantOpThread)) return false;
        if (o.getClass() != getClass()) return o.equals(this);
        return content.equals(((ConstantOpThread) o).content)
               && vars.equals(((ConstantOpThread) o).vars);
    }

    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        // Do nothing
    }

    /**
     * @see niagara.query_engine.SchemaProducer#getTupleSchema()
     */
    public TupleSchema getTupleSchema() {
        TupleSchema ts = new TupleSchema();
        ts.addMappings(vars);
        return ts;
    }
    
    /**
     * @see niagara.utils.SerializableToXML#dumpAttributesInXML(StringBuffer)
     */
    public void dumpAttributesInXML(StringBuffer sb) {
        if (vars.size() == 0) {
            sb.append(">");
            return;
        }
        sb.append(" vars='").append(vars.get(0).getName());
        
        for (int i = 1; i < vars.size(); i++) 
            sb.append(",").append(vars.get(i).getName());
     
        sb.append("'>");   
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
     */
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(content).append("</").append(getName()).append(">");
    }
}


