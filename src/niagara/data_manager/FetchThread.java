
/**********************************************************************
  $Id: FetchThread.java,v 1.9 2003/03/08 01:01:53 vpapad Exp $


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

/** Niagra DataManager
  * FetchThread takes a FetchRequest and service it
  */

import java.util.*;
import org.w3c.dom.*;

import niagara.utils.*;


class FetchThread implements Runnable {
    private FetchRequest req;
    private Thread thr;
    private int blockCount;
    private MemCache cache;

    FetchThread(FetchRequest req, MemCache dmc) {
        this.req = req;
        this.cache = dmc;
        blockCount = 0;
        thr = new Thread(this, "FetchThread");
        thr.start();
        return;
    }

    public void run() {
        Vector tmpUrl = new Vector();
        Object ret = null;
        blockCount = 0;
	try {
	    for(int i=0; i<req.urls.size(); i++) {
                ret = cache.fetch(req.urls.elementAt(i), this);
		if(ret == null) {
		    blockCount++;
		    tmpUrl.add(req.urls.elementAt(i));
		} else {
		    Element tmpele = ((Document)ret).getDocumentElement();
		    if(tmpele==null) {
			throw new PEException("Null document found");
		    }
		    req.s.put((Node)ret);
		}
	    }
	outer:	while(tmpUrl.size()!=0) {
	    if(!notified) _wait();
	    
	    for(int i=0; i<tmpUrl.size(); i++) {
		ret = null;
		Object obj = tmpUrl.elementAt(i);
		ret = MemCache._entryHash.get(obj);
		if(ret != null) { 
		    Object val = ((MemCacheEntry)ret).val;
		    if(val!=null) {
			req.s.put((Node)val);
		    } else {
			System.err.println("HOW DO YOU GET HERE? " + obj.toString());
		    }
		    blockCount--;			
		    tmpUrl.removeElement(obj);
		    cache._add(obj, this);
		    continue outer;
		}
	    }
	    notified = false;	
        }
	} catch(CacheFetchException cfe) {
	    throw new PEException(" !!! *** Fetch Failed");
	} catch (InterruptedException ie) {
	    try {
		req.s.putCtrlMsg(CtrlFlags.SHUTDOWN, "Interrupted");
	    } catch (ShutdownException se) {  // ignore
	    } catch (InterruptedException ine) {} // ignore
	} catch (ShutdownException se) {
	    // nothing to do since our one output stream already got shutdown
	} 

	// All Fetch Done!
	try {
	    req.s.endOfStream();
	} catch (InterruptedException e) { 
	    // ignore...
	} catch (ShutdownException se) {
	    // ignore...
	}
    }

    volatile boolean notified = false;
    public synchronized void _notify() {
	notified = true;
	notifyAll();
    }

    public synchronized void _wait() {
	try {
	    wait();
	    // should implement time out stuff ?
	} catch (InterruptedException ie) {
	    // how to gracefully exit?
	}
    }

   
}


