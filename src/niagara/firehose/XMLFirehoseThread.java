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
    private BufferedWriter client_writer;
    private FirehoseSpec fhSpec;
    private File temp_file;

    // for managing the rate
    private int writtenBytes = 0;
    private boolean useRate;
    private int bytesPerSec;
    private long startTime;

    private int totalBytes = 0; // count total number of bytes written
    private int byteDiff = 0;	// 

    public XMLFirehoseThread(String str, MsgQueue _queue) {
	super(str);
	msg_queue = _queue;
    }
  
    public void run() {
	while(true) {
	    try {
		// get a message from the queue
		msg = msg_queue.get();
		totalBytes = 0;

		fhSpec = msg.getSpec();
		boolean useStreamingFormat = fhSpec.useStreamFormat();
		boolean usePrettyPrint = fhSpec.isPrettyPrint();
		int numGenCalls = fhSpec.getNumGenCalls();

		bytesPerSec = fhSpec.getRate()*1024; // rate in KB/sec
		useRate = true;
		if(bytesPerSec == 0)
		    useRate = false;

		String trace = fhSpec.getTrace();
		BufferedWriter bwTrace = null;
		if (trace.length() != 0)
		    bwTrace =
			new BufferedWriter(new FileWriter(trace, false));
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
							    bwTrace);
		    break;
		case FirehoseConstants.AUCTION_STREAM:
		    xml_generator = new XMLAuctionStreamGenerator(fhSpec.getNumTLElts(),
								  numGenCalls,
								  useStreamingFormat, 
								  usePrettyPrint,
								  bwTrace);
		    break;
		case FirehoseConstants.PACKET:
		    System.err.println("Generating Packets");
		    xml_generator = new XMLPacketGenerator(fhSpec.getDescriptor(),
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
		case FirehoseConstants.FILE:
		    xml_generator = new XMLFileReader(fhSpec.getDescriptor(),
						      useStreamingFormat);
		default:
		    throw new PEException("KT unexpected stream data type");
		}

		if(xml_generator.generatesChars()) {
		    client_writer = new BufferedWriter(new OutputStreamWriter(msg.get_client_socket().getOutputStream()));
		    client_out = null;
		} else { // must generate bytes
		    // set up a writer associated with the socket
		    client_out = new BufferedOutputStream(msg.get_client_socket().getOutputStream());
		    client_writer = null;
		}
		


		int count = 0; // keep track of number of calls to generator
		startTime = System.currentTimeMillis();

		// generate and and send the documents 
		if(useStreamingFormat)
		    write_string(FirehoseConstants.OPEN_STREAM);
		
		if(xml_generator.generatesStream()) {
		    xml_generator.generateStream(this);
		} else {
		    while((count < numGenCalls || numGenCalls == -1) &&
			  xml_generator.getEOF() == false) {
			// write the generated data respecting the rate
			write_string(xml_generator.generateXMLString());
			count++;
		    }
		}
		if(useStreamingFormat)
		    write_string(FirehoseConstants.CLOSE_STREAM);

		if (bwTrace != null)
		    bwTrace.close();
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
	    } 

	    // prepare for next message
	    xml_generator = null; 
	    try {
		System.err.println("Closing connection wrote " + totalBytes + " bytes.");
		if(client_out != null) {
		    client_out.close();
		    client_out = null;
		} else {
		    client_writer.close();
		    client_writer = null;
		}
		msg.get_client_socket().close();
		if(temp_file != null && temp_file.exists()) {
		   temp_file.delete();
		   temp_file =  null;
		}
	    } catch (IOException e2) {
		// nothing to do... we just continue and hope for the best
		System.err.println("Ignoring error..." + e2.getMessage());
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

    protected void write_string(String str) throws IOException {
	write_string(str, str.length());
    }

    protected void write_string(String str, int len) throws IOException {
	if(client_writer == null)
	    throw new PEException("write chars may only be called by generators that generate chars - check generatesChars() method");

	if(XMLFirehose.verbose) {
	    System.out.print(str); 
	}

	totalBytes += len; // keep track of the total number of bytes written

	if(!useRate) {
	    client_writer.write(str, 0, len);
	} else {
	    int leftToWrite;
	    if(len+writtenBytes > bytesPerSec) {
		int toWrite = bytesPerSec - writtenBytes;
		client_writer.write(str, 0, toWrite);
		leftToWrite = len-toWrite;
		writtenBytes += toWrite;
	    } else {
		client_writer.write(str, 0 , len);
		leftToWrite = 0;
		writtenBytes += len;
	    }
	    
	    if(writtenBytes == bytesPerSec) {
		long curTime = System.currentTimeMillis();
		if((curTime - startTime) < 1000) { // 1000 milliseconds in one second
		    try {
			System.err.println("sleeping");
			Thread.sleep(1000 - (curTime-startTime));
		    } catch (java.lang.InterruptedException e) {
			// 
			System.err.println("KT: HELP Got Interrupted Exception in XMLFirehoseThread - IGNORING IT"); // uugh, don't know what else to do
		    }
		} else {
		    System.err.println("KT: WARNING: Data generation is too slow to keep up with rate");
		}
		// start another second
		startTime = System.currentTimeMillis();
		writtenBytes = 0;
	    }
	    
	    if(leftToWrite != 0) {
		if(leftToWrite <= bytesPerSec) {
		    client_writer.write(str, len-leftToWrite, leftToWrite);
		    writtenBytes += leftToWrite;
		} else {
		    throw new PEException("KT: uugh, generated more than one seconds worth of data!!");
		}
	    }
	}		
		
    }

    protected void write_chars(char[] cbuf, int len) throws IOException {
	if(client_writer == null)
	    throw new PEException("write chars may only be called by generators that generate chars - check generatesChars() method");

	totalBytes += len; // keep track of the total number of bytes written
	
	byteDiff += (Array.getLength(cbuf) - len);

	if(XMLFirehose.verbose) {
	    // KT - have to loop since I can't do
	    // System.out.print(cbuf, len)
	    for(int i = 0; i<len; i++) {
		System.out.print(Array.get(cbuf, i));
	    }
	}

	if(!useRate) {
	    client_writer.write(cbuf, 0, len);
	} else {
	    int leftToWrite;
	    if(len+writtenBytes > bytesPerSec) {
		int toWrite = bytesPerSec - writtenBytes;
		client_writer.write(cbuf, 0, toWrite);
		leftToWrite = len-toWrite;
		writtenBytes += toWrite;
	    } else {
		client_writer.write(cbuf, 0 , len);
		leftToWrite = 0;
		writtenBytes += len;
	    }
	    
	    if(writtenBytes == bytesPerSec) {
		long curTime = System.currentTimeMillis();
		if((curTime - startTime) < 1000) { // 1000 milliseconds in one second
		    try {
			System.err.println("sleeping");
			Thread.sleep(1000 - (curTime-startTime));
		    } catch (java.lang.InterruptedException e) {
			// 
			System.err.println("KT: HELP Got Interrupted Exception in XMLFirehoseThread - IGNORING IT"); // uugh, don't know what else to do
		    }
		} else {
		    System.err.println("KT: WARNING: Data generation is too slow to keep up with rate");
		}
		// start another second
		startTime = System.currentTimeMillis();
		writtenBytes = 0;
	    }
	    
	    if(leftToWrite != 0) {
		if(leftToWrite <= bytesPerSec) {
		    client_writer.write(cbuf, len-leftToWrite, leftToWrite);
		    writtenBytes += leftToWrite;
		} else {
		    throw new PEException("KT: uugh, generated more than one seconds worth of data!!");
		}
	    }
	}		
		
    }

    // writes bytes to stream obeying the proper rate
    protected void write_bytes(byte[] genBytes, int numGenBytes) throws IOException {
	if(client_out == null)
	    throw new PEException("KT invalid call");

	totalBytes += numGenBytes; // keep track of the total number of bytes written

	if(!useRate) {
	    client_out.write(genBytes, 0, numGenBytes);		
	} else {
	    int leftToWrite;
	    if(numGenBytes+writtenBytes > bytesPerSec) {
		int toWrite = bytesPerSec - writtenBytes;
		client_out.write(genBytes, 0, toWrite);
		leftToWrite = numGenBytes-toWrite;
		writtenBytes += toWrite;
	    } else {
		client_out.write(genBytes, 0, numGenBytes);
		leftToWrite = 0;
		writtenBytes += numGenBytes;
	    }
	    
	    if(writtenBytes == bytesPerSec) {
		long curTime = System.currentTimeMillis();
		if((curTime - startTime) < 1000) { // 1000 milliseconds in one second
		    try {
			System.err.println("sleeping");
			Thread.sleep(1000 - (curTime-startTime));
		    } catch (java.lang.InterruptedException e) {
			// 
			System.err.println("KT: HELP Got Interrupted Exception in XMLFirehoseThread - IGNORING IT"); // uugh, don't know what else to do
		    }
		} else {
		    System.err.println("KT: WARNING: Data generation is too slow to keep up with rate");
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
}


