/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


package niagara.data_manager;

/** Niagra DataManager
  * FirehoseThread - connect to the firehose - retrieve data and
  * put it into the stream
  * Based on FetchThread
  */

import org.w3c.dom.*;
import java.io.*;
import org.xml.sax.*;

import niagara.ndom.*;

import niagara.query_engine.*;
import niagara.utils.*;
import FirehoseClient;
import niagara.xmlql_parser.op_tree.FirehoseSpec;


public class FirehoseThread implements Runnable {
    private FirehoseSpec spec;
    private SinkTupleStream outputStream;
    private FirehoseClient fhClient;
    private ByteArrayInputStream messageStream;

    public FirehoseThread(FirehoseSpec spec, SinkTupleStream outStream) {
	this.spec = spec;
	outputStream = outStream;
    }

    /**
     * Thread run method
     *
     */
    public void run() {

	try {
	    boolean messageEOF = false;
	    fhClient = new FirehoseClient(spec.getListenerPortNum(),
					  spec.getListenerHostName());
	    int err = fhClient.open_stream(spec.getRate(), spec.getType(),
					   spec.getDescriptor(), spec.getIters(),
					   spec.getDocCount());
	    
	    long mem = Runtime.getRuntime().totalMemory();
	    System.out.println("Firehose - starting mem size " + mem);
	    
	    while(err == 0 && messageEOF == false) {
		err = fhClient.get_document(); // force client to return whole doc
		if(err == 0) {
		    messageEOF = fhClient.get_eof();
		    
		    /* buffer assumed to contain exactly one document
		     * certainly true if get_document used 
		     * Note the last doc should be waiting when we get
		     * the eof notification
		     */
		    messageStream = fhClient.get_doc_stream();
		    
		    /* processMessageBuffer returns false when
		     * the stream gets a shutdown message (from
		     * an operator above
		     */
		    processMessageBuffer();
		}
	    }
	// Our work is finished
	if(!outputStream.isClosed())
	    outputStream.endOfStream();
	spec = null;
	outputStream = null;
	fhClient = null;
	messageStream = null;

	} catch (InterruptedException e) {
	    throw new PEException("think this shouldn't happen");
	} catch (ShutdownException e) {
	    spec = null;
	    outputStream = null;
	    fhClient = null;
	    messageStream = null;
	} catch (SAXException se) {
	    System.err.println("SAXException in FirehoseThread: " +
			       se.getMessage());
	} catch (java.io.IOException ioe) {
	    System.err.println("IOException in FirehoseThread: " +
			       ioe.getMessage());
	}

	return;
    }

    /** process the incoming message which was just put in the message
     * buffer - this means parse it and put the resulting Document
     * in the output stream
     */
    private void processMessageBuffer() 
	throws InterruptedException, ShutdownException,
	       SAXException, IOException {
	Document parsedDoc = parseMessageBuffer();

	if(parsedDoc == null) {
	    throw new PEException("null doc found");
	}

	outputStream.put(parsedDoc);
    }

    // We are trusting the firehose to return valid documents
    niagara.ndom.DOMParser parser;

    /**
     * parse the messageBuffer 
     *
     * @return returns the parsed Document
     */
    private Document parseMessageBuffer() 
	throws SAXException, IOException {
	/* parse the messageBuffer 
	 * Note: messageBuffer may be longer than actual message 
	 */

	if (parser == null) {
	    parser = DOMFactory.newParser();
	}
	parser.parse(new InputSource(messageStream));
	Document doc = parser.getDocument();
	return doc;
    }
	    
    /* need a new function on fhClient to say - I'm going away
     * and am not going to read any more data in the stream -
     * will this work? or will we leave sockets, etc. laying around 
     */
    private void close_firehose_stream() {
	// this is the best we can do for now
	fhClient.request_stream_close();
	return;
    }
  
}


