/**
 * $Id: ConstantOpThread.java,v 1.5 2002/05/23 06:31:00 vpapad Exp $
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
import niagara.ndom.*;

public class ConstantOpThread implements Runnable {
    private SinkTupleStream outputStream;

    private String content;

    public ConstantOpThread(String content, SinkTupleStream outStream) {
	this.content = content;
	outputStream = outStream;
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

	return;
    }

    /** process the incoming message which was just put in the message
     * buffer - this means parse it and put the resulting Document
     * in the output stream
     */
    private void processDocument() 
	throws SAXException, ShutdownException, IOException {
	try {
            niagara.ndom.DOMParser parser = DOMFactory.newParser();
	    parser.parse(new InputSource(new ByteArrayInputStream(content.getBytes())));
	    Document doc = parser.getDocument();


            if (doc.getDocumentElement().getTagName().equals("stream")) {
                Hashtable elements = new Hashtable();
                processStreamDoc(doc.getDocumentElement(), elements);
                
                NodeList tuples = doc.getDocumentElement().getElementsByTagName("tuple");
                for (int i = 0; i < tuples.getLength(); i++) {
                    Element tuple = (Element) tuples.item(i);
                    NodeList attrs = tuple.getElementsByTagName("elt");
                    StreamTupleElement ste = new StreamTupleElement(false);
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
}


