/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


package niagara.data_manager;

/** Niagra DataManager
 * StreamThread - an input thread to take a simple stream of XML documents
 * from a file or a socket, for now will just implement the file???
 * Documents in stream are not demarcated, uses a modified xerces
 * parser to parse each document one at a time and put each parsed
 * document into the output stream
 */

import org.w3c.dom.*;
import java.io.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
import java.net.Socket;
import java.lang.reflect.Array;
import javax.xml.parsers.*;
import com.microstar.xml.*;

import niagara.ndom.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
// KT ugly!! for stream variable - sorry!!!
import niagara.connection_server.NiagraServer; 

public class StreamThread implements Runnable {
    private StreamSpec spec;
    private InputStream inputStream;
    private SourceStream outputStream;
    private niagara.ndom.DOMParser parser;
    //private javax.xml.parsers.SAXParser parser;
    //private com.microstar.xml.SAXDriver parser;
    private Socket firehoseSocket;
    private char c;
    private BufferedReader bufferedInput = null;
    private MyArray buffer; 
    private int idx;
    private StreamSimpleHandler handler;

    public StreamThread(StreamSpec spec, SourceStream outStream) {

	//try {
	    this.spec = spec;
	    outputStream = outStream;
	    //parser = new com.microstar.xml.SAXDriver();
	    parser = DOMFactory.newParser("saxdom");
	    //parser = SAXParserFactory.newInstance().newSAXParser();
	    firehoseSocket = null;
	    //handler = new StreamSimpleHandler();
	    //parser.setHandler(handler);
	    //} catch (javax.xml.parsers.ParserConfigurationException pce) {
	    //System.err.println("KT parser configuration error " + 
	    // 		       pce.getMessage());
	    //} catch(org.xml.sax.SAXException se) {
	    //System.err.println("KT sax exception " + se.getMessage());
	    //}
    }

    /**
     * Thread run method
     *
     */
    public void run() {
	boolean sourcecreated = false;

	try {
	    inputStream = createInputStream();
	   
	    if(false) {
	    //if(NiagraServer.stream && parser.supportsStreaming()) {
		// stream is done when inputStream.read() returns -1
		boolean keepgoing = true;
		InputSource inputSource = new InputSource(inputStream);
		sourcecreated=true;
		while(keepgoing) {
		    parser.parse(inputSource);
		    //keepgoing = outputStream.put(parser.getDocument());
		}	
	    } else {
		try {
		    bufferedInput = 
			new BufferedReader(new InputStreamReader(inputStream));
		    buffer = new MyArray(8192);

		    idx = 0;
		    boolean keepgoing = true;
		    boolean startOfDoc = false;
		    
		    char cbuf[] = new char[5];
		    bufferedInput.read(cbuf, 0, 5);
		    if(cbuf[0] != '<' || cbuf[1] != '?' || cbuf[2] != 'x' ||
		       cbuf[3] != 'm' || cbuf[4] != 'l')
			throw new DMException("Invalidly formed stream");
		    
		    int cbufLen = 0;
		    
		    while(keepgoing) {
			idx = 0;
			startOfDoc = false;
			cbufLen = 0;
			
			// at this point cbuf should always hold <?xml
			for(int i = 0; i<5; i++) {
			    buffer.setChar(idx, cbuf[i]);
			    idx++;
			}
			
			while(!startOfDoc) {
			    // look for string <?xml - indicates start of NEXT document
			    boolean found = false;

			    // if readChar returns true - indicates endOfStream
			    // puts character read in c
			    if(!readCharAndCheck('<')) {
				buffer.setChar(idx, c);
				idx++;
			    } else {
				cbuf[0] = c;
				cbufLen = 1;
				found = readCharAndCheck('?');
				cbuf[1] = c;
				cbufLen = 2;
				if(found) {
				    found = readCharAndCheck('x');
				    cbuf[2] = c;
				    cbufLen = 3;
				    if(found) {
					found = readCharAndCheck('m');
					cbuf[3] = c;
					cbufLen = 4;
					if(found) {
					    found = readCharAndCheck('l');
					    cbuf[4] = c;
					    cbufLen = 5;
					    if(found) {
						startOfDoc = true;
					    }
					}
				    }
				}
				if(!startOfDoc) {
				    for(int i = 0; i<cbufLen; i++) {
					buffer.setChar(idx+i, cbuf[i]);
				    }
				    idx+=cbufLen;
				}
			    }
			}
			
			// parse the buffer and put the resulting document
			// in the output stream
			keepgoing = parseAndSendBuffer(buffer, idx);
		    }
		} catch(EOSException eosE) {
		    // parse final doc and put in output stream, then return
		    parseAndSendBuffer(buffer, idx);
		}
	    }
	    
	} catch (org.xml.sax.SAXException saxE){
	    System.err.println("StreamThread::SAX exception parsing document. Message: " 
			       + saxE.getMessage());
	} catch (java.lang.InterruptedException intE) {
	    System.err.println("StreamThread::Interruped Exception::run. Message: " 
			       + intE.getMessage());
	} catch (niagara.utils.StreamPreviouslyClosedException spcE) {
	    System.err.println("StreamThread::Stream previously closed exception::run Message: " 
			       + spcE.getMessage());
	} catch (java.io.FileNotFoundException fnfE) {
	    System.err.println("StreamThread::File not found: filename: " 
			       + spec.getFileName() +
			       "Message" + fnfE.getMessage());
	} catch (java.net.UnknownHostException unhE) {
	    System.err.println("StreamThread::Unknown Host: host: " + spec.getHostName() + 
			       "Message" + unhE.getMessage());
	} catch(java.io.IOException ioe) {
	    if(!sourcecreated) {
	       System.err.println("StreamThread::IOException. Message: " + ioe.getMessage());
	     }
	     // if source was created IOException tends to mean end
	     // of stream and should be ignored
	} catch(niagara.utils.NullElementException neE) {
	    System.err.println("StreamThread::Null element exception. Message " +
			       neE.getMessage());
	} catch(niagara.data_manager.DMException dmE) {
	    System.err.println("StreamThread::Stream Exception. Message " +
			       dmE.getMessage());
	} 

	cleanUp();

	return;
    }

    private void cleanUp() {

	try {
	    closeInputStream();
	} catch(java.io.IOException ioe) {
	    /* do nothing */
	}

	try {
	    outputStream.close();
	} catch(niagara.utils.StreamPreviouslyClosedException e) {
	    /* do nothing */
	}

	spec = null;
	outputStream = null;
	inputStream = null;
	parser = null;
	
	return;
    }

    /**
     * Create an input stream from either a file or a socket
     */
    private InputStream createInputStream() 
	throws java.io.FileNotFoundException, 
	       java.net.UnknownHostException,
	       java.io.IOException {
	if(spec.getType() == StreamSpec.FILE) {
	    return createFileStream();
	} else if (spec.getType() == StreamSpec.FIREHOSE) {
	    return createFirehoseStream();
	} else {
	    throw new PEException("Invalid Stream Type");
	}
    }

    /** 
     * create an input stream from the file name given
     */
    private java.io.InputStream createFileStream() 
    throws java.io.FileNotFoundException{
	//System.out.println("KT: Creating file stream for " + spec.getFileName());
	return new FileInputStream(spec.getFileName());
    }

    /** 
     * create an input stream from the socket given
     */
    private java.io.InputStream createFirehoseStream() 
	throws java.net.UnknownHostException, java.io.IOException {
	firehoseSocket = new Socket(spec.getHostName(), spec.getPortNum());
	return firehoseSocket.getInputStream();
    }
     
    /**
     * Indicate I'm going away and am not going to read any more data 
     * in the stream - to try to close up sockets, etc.
     */
    private void closeInputStream() 
	throws java.io.IOException {
	if(spec.getType() == StreamSpec.FILE) {
	    closeFileStream();
	} else if (spec.getType() == StreamSpec.FIREHOSE) {
	    closeFirehoseStream();
	} else {
	    throw new PEException("Invalid Stream Type");
	}

	return;
    }

    private void closeFileStream() 
	throws java.io.IOException {
	if(inputStream != null) {
	    inputStream.close();
	}
    }

    private void closeFirehoseStream() 
	throws java.io.IOException {
	if(inputStream != null) {
	    inputStream.close();
	}
	if(firehoseSocket != null) {
	    firehoseSocket.close();
	}
    }
  
    /** 
     * Reads a character from the stream, checks for end of stream,
     * puts the character read in c, checks to see if the character
     * matches the checkVal
     * returns true if matches checkVal, false otherwise
     */
    private boolean readCharAndCheck(char checkVal) 
    throws EOSException, java.io.IOException {
	int i = bufferedInput.read();
	if(i == -1) {
	    throw new EOSException();
	}
	c = (char)i;
	if(c == checkVal)
	    return true;
	else
	    return false;
    }

    private boolean parseAndSendBuffer(MyArray buffer, int idx) 
	throws org.xml.sax.SAXException, java.lang.InterruptedException,
	       java.io.IOException, niagara.utils.NullElementException,
	       niagara.utils.StreamPreviouslyClosedException {
	InputSource inputSource = 
	    new InputSource(new CharArrayReader(buffer.getBuf(), 0, idx));
	parser.parse(inputSource);
	//return true;
	return outputStream.put(parser.getDocument());
    }

    private class EOSException extends Exception {
	public EOSException() {
	    super("End of Stream Exception");
	}
    }

    // simple array 
    class MyArray {
	char buffer[];
	int len;

	MyArray() {
	    this(8192);
	}

	MyArray(int size) {
	    buffer = new char[8192];
	    len = 8192;
	}

	void setChar(int idx, char c) {
	    if(idx >= len) {
		ensureCapacity(idx);
	    }
	    buffer[idx] = c;
	}

	void ensureCapacity(int idx) {
	    if(idx < len)
		return;
	    int oldlen = len;
	    while(idx >= len) {
		len *=2;
	    }
	    char newBuffer[] = new char[len];
	    System.arraycopy(buffer, 0, newBuffer, 0, oldlen);
	    buffer = newBuffer;
	    return;
	}

	char[] getBuf() {
	    return buffer;
	}

    }
}



class StreamSimpleHandler extends DefaultHandler {

    public StreamSimpleHandler() {
    }

    public void startDocument() {
	//System.out.println("Stream start document");
    }

    public void endDocument() {
	//System.out.println("Stream end document");
    }

}
