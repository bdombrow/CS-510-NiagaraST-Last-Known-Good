
/**********************************************************************
  $Id: SimpleConnectionReader.java,v 1.1 2000/07/09 05:38:54 vpapad Exp $


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

import java.net.*;
import java.io.*;

import gnu.regexp.*;

class SimpleConnectionReader extends AbstractConnectionReader 
    implements Runnable {
    UIDriverIF ui;

    public SimpleConnectionReader(String hostname, int port, 
				  UIDriverIF ui, DTDCache dtdCache) {
	super(hostname, port, ui, dtdCache);
	this.ui = ui;
    }

    /**
     * The run method accumulates result strings
     */
    public void run()   {
	// Read the connection and throw the callbacks
	
	try {
	    BufferedReader br = new BufferedReader(cReader);
	    String line;
	    RE re = new RE("<responseMessage localID\\s*=\\s*\"([0-9]*)\"\\s*serverID\\s*=\\s*\"([0-9]*)\"\\s*responseType\\s*=\\s*\"server_query_id\"");
	    boolean registered = false;
	    int local_id = -1, server_id = -1;
	    do {
		line = br.readLine();
		if (line.indexOf("<response") == 0  || line.indexOf("</response") == 0) {
		    if (!registered && line.indexOf("\"server_query_id\"") != -1) {
			REMatch m = re.getMatch(line);
			local_id = Integer.parseInt(m.substituteInto("$1"));
			server_id = Integer.parseInt(m.substituteInto("$2"));
			queryRegistry.setServerId(local_id, server_id);
		    }
		    if (line.indexOf("\"end_result\">") != -1)
			ui.notifyFinalResult(local_id);
		}
		else System.out.println(line);
	    } while (line != null);
	}
	catch(Exception e){
	    System.err.println("An exception in the server connection");
	    e.printStackTrace();
	    System.exit(-1);
	}
    }
}











