/**********************************************************************
  $Id: RequestParser.java,v 1.8 2003/02/25 06:16:28 vpapad Exp $


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

import java.io.*;
import java.util.*;

import gnu.regexp.*;

import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
import com.microstar.xml.SAXDriver;

import niagara.utils.*;
import niagara.trigger_engine.TRIGException;
import niagara.query_engine.QueryResult;

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

    //local variables

    // The request Handler that created this object and should be called
    // whenever a RequestMessage is received
    private RequestHandler reqHandler;   
    private Thread parserThread;
    
    // contains the current element being processed by the parser
    private String currentElement;
    //The current request message that is being built
    private RequestMessage currentMesg;
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
	     currentMesg.requestType = atts.getValue(REQUEST_TYPE);
	     currentMesg.requestData = "";
	 } else if (name.equals(REQUEST)) { 
	     // do nothing, this is the first tag
	 } else if (currentMesg == null) {
	     // REQUEST MESSAGE tag should come first - this is just for debugging
	     throw new PEException("KT - think this shouldn't happen RequestParser::startElement. Element tag is " + name);
	 }
     }

    //If an element ends
    public void endElement(String name) {
	// if the request ended then it means that we have successfully parsed the request
	if (name.equals(REQUEST_MESSAGE)) {
	    // lets handle the request - this has to happen here - parser handles
	    // multiple messages from client
	    int err_type;
	    boolean error;
	    String message;

	    // handle errors in this function
	    // so we can return better messages than if
	    // we just send up a sax exception	    
	    try {
		reqHandler.handleRequest(currentMesg);
		// keep compiler happy and catch pgming errors below
		err_type = 0;
		error = false;
		message = null;
	    } catch (RequestHandler.InvalidQueryIDException e) {
		error = true;
		err_type = ResponseMessage.INVALID_SERVER_ID;
		message = e.getMessage();
	    } catch (InvalidPlanException e) {
		error = true;
		err_type = ResponseMessage.PARSE_ERROR;
		message = e.getMessage();
	    } catch (QueryResult.ResultsAlreadyReturnedException e) {
		error = true;
		err_type = ResponseMessage.EXECUTION_ERROR;
		message = e.getMessage();
	    } catch (QueryResult.AlreadyReturningPartialException e) {
		error = true;
		err_type = ResponseMessage.EXECUTION_ERROR;
		message = e.getMessage();
	    } catch (ShutdownException e) {
		error = true;
		err_type = ResponseMessage.EXECUTION_ERROR;
		message = "System was shutdown during query";
	    } catch (TRIGException e) {
		error = true;
		err_type = ResponseMessage.ERROR;
		message = e.getMessage();
	    } catch (IOException e) {
		error = true;
		err_type = ResponseMessage.ERROR;
		message = "IOException during query: " + e.getMessage();
	    } catch (RuntimeException e) {
		// deal with runtime exceptions here - if not these are
		// turned into SAX exceptions by the parser
		sendErrMessage(ResponseMessage.ERROR, 
			    "Programming or Runtime Error (see server for message)");
		System.err.println("WARNING: PROGRAMMING or RUNTIME ERROR " + e.getMessage());
		e.printStackTrace();
		// keep compiler happy
		error = false;
		err_type = -1;
		message = null;
		// kill system as would be expected on a runtime exception
		System.exit(0);
	    } 

	    if(error) {
		System.err.println("\nERROR occured during query parsing or execution. Error Message: " + message);
		System.err.println();
		sendErrMessage(err_type, message);
	    }
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

    private void sendErrMessage(int err_type, String message) {
	try {
	    if(currentMesg == null) {
		// just do something stupid, anything, to get message back to client
		// if currentMesg is null, means error occured so early in parsing
		// of the request message that localID and serverID could not be read
		currentMesg = new RequestMessage();
		currentMesg.localID = -1;
		currentMesg.serverID = -1;
	    }
	    ResponseMessage rm = 
		new ResponseMessage(currentMesg, err_type);
	    // add local id here in case padding is turned off !*#$*@$*
	    rm.setData("SERVER ERROR - localID=\"" + currentMesg.localID 
	               + "\" - Error Message: " + message);
	    reqHandler.sendResponse(rm);
	    reqHandler.closeConnection();
	} catch (IOException ioe) {
	    System.err.println("\nERROR sending message \"" + message + "\" to client " +
			       "Error message: " + ioe.getMessage());
	    ioe.printStackTrace();
	}
	return;
    }
}
				  
