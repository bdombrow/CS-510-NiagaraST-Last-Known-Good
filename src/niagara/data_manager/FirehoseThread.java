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
    private SourceStream outputStream;
    private FirehoseClient fhClient;
    private String messageBuffer;
    private int messageLen;

    public FirehoseThread(FirehoseSpec spec, SourceStream outStream) {
	this.spec = spec;
	outputStream = outStream;
    }

    /**
     * Thread run method
     *
     */
    public void run() {

	boolean messageEOF = false;
	boolean streamShutdown = false;
	fhClient = new FirehoseClient(spec.getListenerPortNum(),
				      spec.getListenerHostName());
	int err = fhClient.open_stream(spec.getRate(), spec.getType(),
			     spec.getDescriptor(), spec.getIters());
	while(err == 0 && messageEOF == false && streamShutdown == false) {
	    err = fhClient.get_data();
	    if(err == 0) {
		messageEOF = fhClient.get_eof();
		if(messageEOF == false) {
		    messageBuffer = fhClient.get_buffer();
		    messageLen = fhClient.get_len();
		    /* processMessageBuffer returns false when
		     * the stream gets a shutdown message (from
		     * an operator above
		     */
		    streamShutdown = ! processMessageBuffer();
		}
	    }
	}

	// Our work is finished
	try {
	    outputStream.close();
	}
	catch (Exception e) {
	    System.err.println("The output stream for the firehose thread was already closed.");
	}

	// err is not 0 = print out an error message??
	spec = null;
	outputStream = null;
	fhClient = null;
	messageBuffer = null;
	
	return;
    }

    /** process the incoming message which was just put in the message
     * buffer - this means parse it and put the resulting Document
     * in the output stream
     */
    private boolean processMessageBuffer() {
	Document parsedDoc = parseMessageBuffer();
	boolean succeed;
	try {
	    succeed = outputStream.put(parsedDoc);
	} catch(java.lang.InterruptedException e) {
	    System.err.println("Thread interrupted in FirehoseThread::processMessageBuffer");
	    close_firehose_stream();
	    return false;
	} catch (NullElementException e) {
	    System.err.println("NullElementException in FirehoseThread::processMessageBuffer");
	    close_firehose_stream();
	    return false;
	} catch (StreamPreviouslyClosedException e) {
	    System.err.println("StreamPreviouslyClosedException in FirehoseThread::processMessageBuffer");
	    close_firehose_stream();
	    return false;
	}

	if(succeed == false) {
	    /* means we got a shutdown message from the operator above */
	    close_firehose_stream();
	}
	return succeed;
    }

    // We are trusting the firehose to return valid documents
    DOMParser parser;

    /**
     * parse the messageBuffer 
     *
     * @return returns the parsed Document
     */
    private Document parseMessageBuffer() {
	/* parse the messageBuffer 
	 * Note: messageBuffer may be longer than actual message 
	 */

	// Bug in IBM's XMLGenerator
	int i1 = messageBuffer.indexOf("SYSTEM \"\"");
	if (i1 !=  -1) {
	    messageBuffer = messageBuffer.substring(0, i1) + 
		"SYSTEM \"http://www.cse.ogi.edu/~vpapad/xml/play.dtd\"" +
		messageBuffer.substring(i1 + "SYSTEM \"\"".length());
	}

	// Get rid of 0x0's
  	if (messageBuffer.indexOf(0) != -1)
 	    messageBuffer = messageBuffer.substring(0, messageBuffer.indexOf(0));	
	try {
	    if (parser == null) {
  		parser = DOMFactory.newParser();
	    }
	    parser.parse(new InputSource(new ByteArrayInputStream(messageBuffer.getBytes())));
	    Document doc = parser.getDocument();
	    return doc;
	}
	catch (Exception ex) {
	    ex.printStackTrace();
	    return null;
	}
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


