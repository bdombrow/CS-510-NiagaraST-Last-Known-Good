package niagara.firehose;

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.Vector;
import java.lang.reflect.Array;

import niagara.utils.*;

class XMLFirehoseThread extends Thread {
    private XMLFirehoseGen xml_generator;
    private MsgQueue msg_queue;
    private XMLGenMessage msg;
    private BufferedOutputStream client_out;
    private FirehoseSpec fhSpec;
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

		fhSpec = msg.getSpec();
		boolean useStreamingFormat = fhSpec.isStreaming();
		boolean usePrettyPrint = fhSpec.isPrettyPrint();
		boolean trace = fhSpec.getTrace();
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
							    usePrettyPrint,
							    trace);
		    break;
		case FirehoseConstants.PACKET:
		    boolean fLive = false;
		    System.out.println("Generating Packets");
		    if (fhSpec.getDescriptor2() != null)
			fLive = Boolean.valueOf(fhSpec.getDescriptor2()).booleanValue();
		    xml_generator = new XMLPacketGenerator(fhSpec.getDescriptor(),
							   fLive,
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
		int writtenBytes = 0;
		byte[] genBytes;
		int numGenBytes;
		int leftToWrite;

		int bytesPerSec = fhSpec.getRate()*1024; // rate in KB/sec
		boolean useRate = true;
		if(bytesPerSec == 0)
		    useRate = false;
		long startTime = System.currentTimeMillis();

		if(useStreamingFormat)
		    client_out.write(FirehoseConstants.OPEN_STREAM.getBytes());
		while((count < numGenCalls || numGenCalls == -1) &&
		      xml_generator.getEOF() == false) {

		    genBytes = xml_generator.generateXMLBytes();
		    count++;

		    if(!useRate) {
			client_out.write(genBytes);		
		    } else {
			numGenBytes = Array.getLength(genBytes);
			
			if(numGenBytes+writtenBytes > bytesPerSec) {
			    int toWrite = bytesPerSec - writtenBytes;
			    client_out.write(genBytes, 0, toWrite);
			    leftToWrite = numGenBytes-toWrite;
			    writtenBytes += toWrite;
			} else {
			    client_out.write(genBytes);
			    leftToWrite = 0;
			    writtenBytes += numGenBytes;
			}
			
			if(writtenBytes == bytesPerSec) {
			    long curTime = System.currentTimeMillis();
			    if((curTime - startTime) < 1000) { // 1000 milliseconds in one second
				try {
				    Thread.sleep(1000 - (curTime-startTime));
				} catch (java.lang.InterruptedException e) {
				    // 
				    System.err.println("KT: HELP Got Interrupted Exception in XMLFirehoseThread - IGNORING IT"); // uugh, don't know what else to do
				}
			    } else {
				System.out.println("KT: WARNING: Data generation is too slow to keep up with rate");
			    }
			    // start another second
			    startTime = System.currentTimeMillis();
			    writtenBytes = 0;
			}

			if(leftToWrite != 0) {
			    if(leftToWrite <= bytesPerSec) {
				client_out.write(genBytes, numGenBytes-leftToWrite, leftToWrite);
				writtenBytes += leftToWrite;
			    } else {
				throw new PEException("KT: uugh, generated more than one seconds worth of data!!");
			    }
			}
		    }	
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
	    } catch (SecurityException e) {
		// problem opening temp.xml
		System.err.println("ERROR: Security Violation: "
				   + e.getMessage());
	    } catch (EOSException e) {
		// do nothing, just go on
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


