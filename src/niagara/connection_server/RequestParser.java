
/**********************************************************************
  $Id: RequestParser.java,v 1.3 2001/08/08 21:25:05 tufte Exp $


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
    RequestHandler reqHandler;   
    Thread parserThread;
    
    // contains the current element being processed by the parser
    String currentElement;
    //The current request message that is being built
    RequestMessage currentMesg;
    // The source from which the messages in XML are to be read
    InputSource source;
    //XMLParser parser;
    SAXDriver parser;


    /** Constructor
	@param istream The stream from which to read Messages
	@param reqHandler Class for handling Request Messages
    */
    public RequestParser(InputStream istream,RequestHandler reqHandler) {
	this.reqHandler = reqHandler;
	//source = new InputSource(new BufferedReader(new InputStreamReader(istream)));
	source = new InputSource(istream);
	parser = new com.microstar.xml.SAXDriver();
    }

    public void startParsing() {
	parserThread = new Thread(this,"RequestParser");
	parserThread.start();
    }

    public void run() {
	try {
	    //System.out.println("request parser started...");
	    parser.setDocumentHandler(this);
	    parser.parse(source);
	}
	catch (Exception e) {
	    //System.out.println("Parser closed. Shutting down service to this client");
	    reqHandler.closeConnection();
	}
	return;
    }

     public  void startElement(String  name, AttributeList atts) {
	 //	 super.startElement(name,atts);
	 // record which element is currently being processed
	 currentElement = name;
	 // if it is the beginning of requestMessage, get all the attributes outta there
	 if (name.equals(REQUEST_MESSAGE)) {
	     currentMesg = new RequestMessage();
	     currentMesg.localID = Integer.parseInt(atts.getValue(LOCAL_ID));
	     currentMesg.serverID = Integer.parseInt(atts.getValue(SERVER_ID));
	     currentMesg.requestType = atts.getValue(REQUEST_TYPE);
	     currentMesg.requestData = "";
	 }
	 else if (name.equals(REQUEST_DATA)||name.equals(REQUEST)) {
	     
	 }
	 //else
	 //    throw new SAXException("Unexpected Element:"+currentElement+"appeared in the stream");
     }

    //If an element ends
    public void endElement(String name) {
	//	super.endElement(name);
	// if the request ended then it means that we have successfully parsed the request
	if (name.equals(REQUEST_MESSAGE)) {
	    //lets handle the request
	    
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
				  
