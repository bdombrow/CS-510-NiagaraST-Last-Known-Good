
/**********************************************************************
  $Id: UrlFetchThread.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


package niagara.data_manager;

import java.io.*;
import java.net.*;

import java.util.Vector;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.data_manager.XMLDiff.*;


/**
 * The UrlFetchThread class is used to fetch a url to local file.  
 */
public class UrlFetchThread implements Runnable {
	
    private Thread thread;
    private UrlQueue urlQueue;
    private DMCache hCache;
    
    public UrlFetchThread (UrlQueue urlQueue, DMCache hc) {
	
	this.urlQueue = urlQueue;
        hCache = hc;
	thread = new Thread(this);
	thread.start();	
	return;
    }

    /**
     *  This is the run method invoked by the Java thread - it simply grabs 
     *  the next query, executes it, and then repeats.
     */
    public void run () {
	do {
	    UrlQueueElement nextUrl = (UrlQueueElement)urlQueue.getUrl();
            try {
                // System.err.println("URL Fetch Thread running");
                execute(nextUrl);
            } catch (Exception e) {
                // System.err.println("URL Fecth exception");
                e.printStackTrace();
            }
	} while (true);
    }
    public void interrupt() {
        thread.interrupt();
    }

    private boolean execute(UrlQueueElement urlObj) 
    {
        if (urlObj == null) {
            return false;
        }
	String url = urlObj.getUrl();
        MemCacheEntry me = urlObj.getMemCacheEntry();
        
        TXDocument doc = CUtil.parseXML(url);
        if(doc==null) {
            // System.err.println("Fetch URL failed. Use dummy to shutdown");
            doc = new TXDocument();
        }
        
        if(me!=null) {
            me.setval(doc);
            me._notifyWaitingThreads();
        }
        return true;
    }
}
