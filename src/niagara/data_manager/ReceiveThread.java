/**
 * $Id: ReceiveThread.java,v 1.6 2003/07/09 04:59:43 tufte Exp $
 *
 */

package niagara.data_manager;

/** Niagara DataManager
  * ConstantOpThread - retrieve data from an embedded document and
  * put it into the stream; based on ReceiveThread
  */

import org.w3c.dom.*;
import org.xml.sax.*;

import java.io.*;
import java.net.*;
import java.util.*;


import niagara.utils.*;
import niagara.ndom.*;
import niagara.xmlql_parser.op_tree.ReceiveOp;

//  XXX vpapad: hack to get CVS to compile
public class ReceiveThread /* extends SourceThread */{
    private SinkTupleStream outputStream;

    private ReceiveOp op;

    public ReceiveThread(ReceiveOp op, SinkTupleStream outStream) {
	this.op = op;
	outputStream = outStream;
    }

    int counter = 0;
    /**
     * Thread run method
     *
     */
    public void run() {
	String url_location = "http://" + op.getLocation()
	    + "/servlet/communication?type=get_tuples&id=" + op.getQueryId();

	try {
            // Connect to remote SCS with location and query_id
            // and establish connection
            URL url = new URL(url_location);
            URLConnection connection = url.openConnection();

            BufferedReader in = new BufferedReader(new InputStreamReader(
                connection.getInputStream()));

            StringBuffer sb = new StringBuffer();
            String inputLine;
            in.readLine(); // Skip HTTP response
            while (true) {
                inputLine = in.readLine();
                if (inputLine == null || inputLine.equals("\0\0")) break;
                if (!inputLine.equals("\0"))
                    sb.append(inputLine);
                else {
                    String tuple = sb.toString();
                    if (tuple != null && !tuple.equals("")) {
                        counter++;
                        processReceive(tuple);
                        sb.setLength(0);
                    }
                }
            }
	    outputStream.endOfStream();
            //System.out.println("XXX received : " + counter + " tuples.");
            
	} catch (MalformedURLException e) {
	    System.err.println("Bad URL " + url_location + " " + e.getMessage());
	    e.printStackTrace();
	} catch (java.io.IOException ioe) {
	    System.err.println("Unable to open connection or read from inputstream, or close stream " + ioe.getMessage());
	    ioe.printStackTrace();
	} catch (InterruptedException ie) {
	    // nothing to do, just print a warning and stop
	    System.err.println("Receive thread interrupted "+ ie.getMessage());
	    return;
	} catch (ShutdownException se) {
	    // nothing to do, just print a warning and stop
	    System.err.println("Receive thread interrupted "+ se.getMessage());
	    return;
	}
    }

    private void processReceive(String tuplestr) 
	throws ShutdownException{
	try {
            niagara.ndom.DOMParser parser = DOMFactory.newParser();

	    parser.parse(new InputSource(new ByteArrayInputStream(tuplestr.getBytes())));
	    Document doc =parser.getDocument();
            
            StreamTupleElement ste = new StreamTupleElement(true);

            Element tuple = doc.getDocumentElement();

            boolean useStreamMaterializer = true;
            if (useStreamMaterializer) {
                processStreamDoc(tuple, elements);
            }
            NodeList nl = tuple.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                Element elt = (Element) nl.item(i).getFirstChild();
                ste.appendAttribute(elt);
            }
	    outputStream.putTupleNoCtrlMsg(ste);
	} catch(java.lang.InterruptedException e) {
	    System.err.println("Thread interrupted in ReceiveThread::processMessageBuffer");
	}  catch(org.xml.sax.SAXException se) {
	    se.printStackTrace();
	    System.out.println("erroneous string was:#" + tuplestr + "#");
	    return;
	} catch (java.io.IOException ioe) {
	    ioe.printStackTrace();
	    System.err.println("Thread shutdown received in ReceiveThread::processMessageBuffer");
	    return;
	}
	return;
    }

    Hashtable elements = new Hashtable();

    // XXX vpapad: Must unify with eqv in ConstantOpThread
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


