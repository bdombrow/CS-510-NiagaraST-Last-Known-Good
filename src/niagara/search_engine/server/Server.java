
/**********************************************************************
  $Id: Server.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.server;

import java.io.*;
import java.net.*;
import java.util.*;
import niagara.search_engine.util.*;

/**
 * Search Engine Server.
 * Uses simple command-line text protocol.
 *
 */
public class Server implements Const {
    protected ServerSocket listenSocket=null;
    private boolean keepRunning=true;

    public static void main(String[] args) {
	HttpURLConnection.setFollowRedirects(true); 
	Server server = new Server();
	server.serveQueries();
    }
  
    public Server() {
	//creating socket
	try {
	    //  do init stuff for indexmgr
	    
	    listenSocket = new ServerSocket(SERVER_PORT, MAX_CLIENTS);
	} catch (IOException e) {
	    System.err.println("ERROR: unable to listen on port "+SERVER_PORT+": "+e);
	    System.exit(1);
	}
    }

    public void serveQueries() {
	Socket clientSocket = null;
    
	while(keepRunning) {
	    try {
		clientSocket = listenSocket.accept();
		QueryHandler handler = new QueryHandler(clientSocket);
	
		Thread runner = new Thread(handler);
		runner.start();
	    } catch (IOException e) {
		System.err.println("ERROR: I/O failure at "+new Date()+": "+e);
	    }
	}
    }

    public void stop() {
	keepRunning=false;
    }
}
 

    
    
