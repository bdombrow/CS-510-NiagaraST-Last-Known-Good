/**********************************************************************
  $Id: ConnectionReader.java,v 1.4 2002/10/29 01:56:21 vpapad Exp $


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


package niagara.client;

import com.microstar.xml.XmlParser;
import java.net.*;
import java.io.*;
import niagara.utils.*;

/**
 * This class establishes a connection with the server.
 * Then calls the parse method of the responseHandler 
 * to read the session document.
 * Each session with the server generates a separate 
 * document which conforms to response.dtd. Inisde this document
 * all the client queries are serviced
 *
 */

class ConnectionReader extends AbstractConnectionReader implements Runnable
{
    // member variables
    
    private XmlParser parser;
    
    /**
     * This object handles callbacks for the session document.
     */
    private ResponseHandler responseHandler;

    public ConnectionReader(String hostname, int port, UIDriverIF ui, DTDCache dtdCache) {
	super(hostname, port, ui, dtdCache);
	initialize(ui, dtdCache);
    }

    /**
     * The run method that invokes the parser
     */
    public void run()   {
	// Read the connection and throw the callbacks
	try {
	    System.err.println("Parsing started");
	    parser.parse(null, null, cReader);
	    System.err.println("Parsing finished. Client Exiting.");
	} catch(EOFException e){
	    System.err.println("Server side ended the session");
	    try {
		socket.close();
	    } catch(IOException ee) {
		System.out.println("Error closing socket");
		ee.printStackTrace();
	    }
	    return;
	}
	catch(SocketException e){
	    System.err.println("Parser Closed down the socket");
	    return;
	} catch(Exception e) {
	    throw new PEException("ConnectionReader: error parsing message from server - I think - KT");
	}
	
	System.exit(0);
    }

    void initialize(UIDriverIF ui, DTDCache dtdCache) {
	// create a response handler and pass to it the registry
	responseHandler = new ResponseHandler(queryRegistry, ui, dtdCache);
	parser = new XmlParser();
	parser.setHandler(responseHandler);
    }
}











