/**********************************************************************
<<<<<<< RequestParser.java
  $Id: RequestParser.java,v 1.16 2007/04/30 23:17:08 jinli Exp $
=======
  $Id: RequestParser.java,v 1.16 2007/04/30 23:17:08 jinli Exp $
>>>>>>> 1.15

  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


package niagara.connection_server;

import gnu.regexp.RE;

import java.io.InputStream;

import niagara.connection_server.RequestMessage.InvalidRequestTypeException;

//import org.xml.sax.AttributeList;
//import org.xml.sax.HandlerBase;
//import org.xml.sax.InputSource;
//import org.xml.sax.SAXException;

import org.xml.sax.*;

import com.microstar.xml.SAXDriver;

/** This class is responsible for creating a SAX Parser for parsing incoming
 * request messages as well as implementing DocumentHandler interface
*/
public class RequestParser extends HandlerBase implements Runnable {
    
    // All the element/attribute names that appear in the Request DTD
    // These have been defined as constants for easy modification
    static final String REQUEST_MESSAGE = "requestMessage";
    static final String LOCAL_ID = "localID";
    static final String SERVER_ID = "serverID";
    static final String REQUEST_TYPE =  "requestType";
    static final String REQUEST_DATA = "requestData";
    static final String REQUEST = "request";
    static final String RESULT_TYPE = "resultType";

    //local variables

    // The request Handler that created this object and should be called
    // whenever a RequestMessage is received
    RequestHandler reqHandler;   
    private Thread parserThread;
    
    // contains the current element being processed by the parser
    private String currentElement;
    //The current request message that is being built
    RequestMessage currentMesg;
    // The source from which the messages in XML are to be read
    private InputSource source;
    private SAXDriver parser;


    /** Constructor
	@param istream The stream from which to read Messages
	@param reqHandler Class for handling Request Messages
    */
    public RequestParser(InputStream istream, RequestHandler reqHandler) {
	this.reqHandler = reqHandler;
	source = new InputSource(istream);
	parser = new com.microstar.xml.SAXDriver();
    }

    public void startParsing() {
	parserThread = new Thread(this,"RequestParser");
	parserThread.start();
    }

    public void run() {
	try {
	    parser.setDocumentHandler(this);
	    parser.parse(source);
	} catch (SAXException saxe) { 
	    // KT - HAVE to handle these sax exceptions - sax parser turns
	    // runtime exceptions into SAX exceptions!!!
	    // cant send a message to client - parser has helpfully closed socket
	    // KT - ok problem here, we get an exception at end of stream - seems
	    // try this ignore message if sax error is null, print if not.
	    if(saxe.getMessage() != null)
		System.err.println("\nERROR parsing request message. SAX error: " +
			 saxe.getMessage() + " Unable to notify client of this error");
	}

	return;
    }

     public  void startElement(String  name, AttributeList atts) {
	 // record which element is currently being processed
	 currentElement = name;
	 // if it is the beginning of requestMessage, get all the attributes outta there
	 if (name.equals(REQUEST_MESSAGE)) {
	     currentMesg = new RequestMessage();
	     currentMesg.localID = Integer.parseInt(atts.getValue(LOCAL_ID));
	     currentMesg.serverID = Integer.parseInt(atts.getValue(SERVER_ID));
	     
	     String outputType = atts.getValue(RESULT_TYPE); 
	     if (outputType != null) {
	    	if (outputType.compareToIgnoreCase("text") == 0)
	    		currentMesg.asii = true;  
	     }
         String reqType = atts.getValue(REQUEST_TYPE);
         try {
             currentMesg.setRequestType(reqType);
         } catch (InvalidRequestTypeException irte) {
             reqHandler.sendErrMessage(currentMesg, ResponseMessage.PARSE_ERROR, "Invalid request type: " + reqType);
         }

	     currentMesg.requestData = "";
	 } else if (name.equals(REQUEST)) { 
	     // do nothing, this is the first tag
	 } else if (currentMesg == null) {
	     // REQUEST MESSAGE tag should come first - this is just for debugging
	     assert false : "KT - think this shouldn't happen RequestParser::startElement. Element tag is " + name;
	 }
     }

    // If an element ends
    public void endElement(String name) {
        // if the request ended then it means that we have successfully parsed
        // the request
        if (name.equals(REQUEST_MESSAGE)) {
            // lets handle the request - this has to happen here - parser
            // handles
            // multiple messages from client
            reqHandler.handleRequest(currentMesg);
        }

        currentElement = "";
    }
	

    //some text data coming in
    public void characters(char[] data,int start, int length) throws SAXException {
	if (!currentElement.equals(REQUEST_DATA)) 
	    return;

	try {
	    String dataString = new String(data,start,length);
            RE re = new RE("ESC]ESC]ESC>");
            dataString = re.substituteAll(dataString, "]]>");
	    currentMesg.requestData += dataString;
	}
	catch (Exception ex) {
	    ex.printStackTrace();
	}
	
    }
}
				  
