/**
 * $Id: ReceiveThread.java,v 1.1 2001/07/17 06:57:55 vpapad Exp $
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


import niagara.query_engine.*;
import niagara.utils.*;
import niagara.ndom.*;

import niagara.xmlql_parser.op_tree.ReceiveOp;

public class ReceiveThread implements Runnable {
    private SourceStream outputStream;

    private ReceiveOp op;

    public ReceiveThread(ReceiveOp op, SourceStream outStream) {
	this.op = op;
	outputStream = outStream;
    }

    int counter = 0;
    /**
     * Thread run method
     *
     */
    public void run() {
	try {
            // Connect to remote SCS with location and query_id
            // and establish connection
            String url_location = "http://" + op.getLocation()
                + "/servlet/communication?type=get_tuples&id=" + op.getQueryId();

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
	    outputStream.close();
            //System.out.println("XXX received : " + counter + " tuples.");
            
	}
	catch (Exception e) {
            System.err.println("Exception while receiving tuples: ");
            e.printStackTrace();
	}
    }

    private boolean processReceive(String tuplestr) {
	boolean succeed;
	try {
            DOMParser parser = DOMFactory.newParser();

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
	    succeed = outputStream.steput(ste);
	} catch(java.lang.InterruptedException e) {
	    System.err.println("Thread interrupted in ReceiveThread::processMessageBuffer");
	    return false;
	} catch (NullElementException e) {
	    System.err.println("NullElementException in ReceiveThread::processMessageBuffer");
	    return false;
	} catch (StreamPreviouslyClosedException e) {
	    System.err.println("StreamPreviouslyClosedException in ReceiveThread::processMessageBuffer");
	    return false;
	}
	catch (Exception ex) {
	    ex.printStackTrace();
            System.out.println("erroneous string was:#" + tuplestr + "#");
	    return false;
	}
	return succeed;
    }

    Hashtable elements = new Hashtable();

    // XXX Must unify with eqv in ConstantOpThread
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


