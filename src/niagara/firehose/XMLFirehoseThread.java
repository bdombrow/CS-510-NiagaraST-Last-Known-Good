package niagara.firehose;

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.Vector;

import niagara.utils.*;

class XMLFirehoseThread extends Thread {
    private XMLFirehoseGen xml_generator;
    private MsgQueue msg_queue;
    private XMLGenMessage msg;
    private BufferedOutputStream client_out;
    private FirehoseSpec fhSpec;

    private String stDoc;
    private File temp_file;
    
    public XMLFirehoseThread(String str, MsgQueue _queue) {
	super(str);
	msg_queue = _queue;
    }
  
    public void run() {
	while(true) {
	    try {
		// get a message from the queue
		msg = msg_queue.get();

		// set up a writer associated with the socket
		client_out = new BufferedOutputStream(msg.get_client_socket().getOutputStream());

		// KT - better name than getDataType!! ???
		fhSpec = msg.getSpec();
		boolean useStreamingFormat = fhSpec.isStreaming();
		boolean usePrettyPrint = fhSpec.isPrettyPrint();
		switch(fhSpec.getDataType()) {
		case FirehoseConstants.XMLB:
		    xml_generator = new XMLBGenerator(fhSpec.getNumTLElts(), 
						      useStreamingFormat,
						      usePrettyPrint);
		    break;
		case FirehoseConstants.SUBFILE:
		    xml_generator = new XMLSubfileGenerator(fhSpec.getDescriptor(), 
							    useStreamingFormat,
							    usePrettyPrint);
		    break;
		case FirehoseConstants.TEMP:
		    xml_generator = new XMLTempGenerator(fhSpec.getDescriptor(), 
							 useStreamingFormat,
							 usePrettyPrint);
		    break;
		case FirehoseConstants.AUCTION:
		    xml_generator = new XMLAuctionGenerator(fhSpec.getDescriptor(), 
							    fhSpec.getDescriptor2(),
							    fhSpec.getNumTLElts(),
							    useStreamingFormat, 
							    usePrettyPrint);
		    break;
		case FirehoseConstants.DTD:
		    // if the dtd_name contains :// assume it is a
		    // URL and process appropriately - if not
		    // assume it is a file name
		    String dtdName;
		    if(fhSpec.getDescriptor().indexOf("://") > 0) {
			dtdName = "temp.dtd";
			write_dtd_to_file(dtdName);
		    } else {
			dtdName = fhSpec.getDescriptor();
		    }
		    xml_generator = new XMLDTDGenerator(dtdName, useStreamingFormat, usePrettyPrint);
		    break;
		default:
		    throw new PEException("KT unexpected stream data type");
		}
		int numGenCalls = fhSpec.getNumGenCalls();

		// generate a and send the documents 
		int count = 0;
		if(useStreamingFormat)
		    client_out.write(FirehoseConstants.OPEN_STREAM.getBytes());
		while(count < numGenCalls || numGenCalls == -1) {
		    stDoc = xml_generator.generateXMLString();
		    client_out.write(stDoc.getBytes());
		    count++; 
		}	
		if(useStreamingFormat)
		    client_out.write(FirehoseConstants.CLOSE_STREAM.getBytes());
	    } catch (FileNotFoundException e) {
		// ugh on error, what should I do??
		// can't send error messages anymore...
		System.err.println("ERROR: DTD File not found: " 
				  + e.getMessage());
	    } catch (MalformedURLException e) {
		System.err.println("ERROR: Malformed URL: "
				   + e.getMessage());
	    } catch (IOException e) {
		// This could be from - reader or writer not
		// being able to be set up or from
		// getContent from the url
		System.err.println("ERROR: IO Problem: " 
				   + e.getMessage());

		// else, there is nothing we can do - we can't 
		// communicate with the client -
		// assume this is a problem with this specific
		// client connection - just continue - socket will be
		// closed below
	    } catch (SecurityException e) {
		// problem opening temp.xml
		System.err.println("ERROR: Security Violation: "
				   + e.getMessage());
	    } 

	    // prepare for next message
	    xml_generator = null;
	    try {
		client_out.close();
		client_out = null;
		msg.get_client_socket().close();
		if(temp_file != null && temp_file.exists()) {
		   temp_file.delete();
		   temp_file =  null;
		}
	    } catch (IOException e2) {
		// nothing to do... we just continue and hope for the best
	    }
	}
    }

    private void write_dtd_to_file(String temp_file_name) 
	throws MalformedURLException, IOException {
	
	URL dtd_url = new URL(fhSpec.getDescriptor());
	InputStreamReader content_stream
	    = new InputStreamReader(dtd_url.openStream());
	
	temp_file = new File(temp_file_name);
	
	PrintWriter output_stream 
	    = new PrintWriter(new FileOutputStream(temp_file));
	
	// output the content
	char buffer[] = new char[4096];
	int len = content_stream.read(buffer, 0, 4096);
	while(len > 0) {
	    output_stream.write(buffer, 0, len);
	    len = content_stream.read(buffer, 0, 4096);
	}
	
	output_stream.close();
	content_stream.close();
	return;
    }

}


