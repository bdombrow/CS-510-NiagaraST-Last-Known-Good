
/**********************************************************************
  $Id: FetchThread.java,v 1.2 2000/08/09 23:53:53 tufte Exp $


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
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.utils.*;
import niagara.data_manager.XMLDiff.*;


class FetchThread implements Runnable {
    private FetchRequest req;
    private Thread thr;
    private int blockCount;
    private MemCache cache;

    FetchThread(FetchRequest req, MemCache dmc) {
        // System.err.println("New a FETCH Thread");
        this.req = req;
        this.cache = dmc;
        blockCount = 0;
        thr = new Thread(this);
        thr.start();
        return;
    }

    public void run() {
        // System.err.println("Fetch thread start running");
        Vector tmpUrl = new Vector();
        Object ret = null;
        blockCount = 0;
        for(int i=0; i<req.urls.size(); i++) {
            try {
                ret = cache.fetch(req.urls.elementAt(i), this);

            } catch(CacheFetchException cfe) {
                System.err.println(" !!! *** Fetch Failed");
                // ret = new TXDocument();
            }
            if(ret == null) {
		// System.out.println("FT:: One Bad Miss " + req.urls.elementAt(i));
                blockCount++;
                tmpUrl.add(req.urls.elementAt(i));
            } else {
                // System.out.println("FT:: MEM/Disk Cache Hit 1" + req.urls.elementAt(i));
                try {
                    Element tmpele = ((Document)ret).getDocumentElement();
                    if(tmpele==null) {
                        // System.err.println("A dummy caught");
                    }
                    else {
                        // System.err.println(" ****************** ");
                        // CUtil.printTree(tmpele, "");
                        // System.err.println(" ****************** ");
                    }
                    req.s.put(ret);
                } catch (Exception se) {
                    System.err.println("Closed Stream in Fetch");
                }
            }
	    /*try {
		System.out.println("Fetch thread sleeping for 5 seconds");
				Thread.sleep(5000);
	} catch (java.lang.InterruptedException ie) {
		System.out.println("Fetch thread interrupted! What does this mean??");
	    }*/
        }
outer:  while(tmpUrl.size()!=0) {
	    if(!notified) _wait();
	    /*
            if(blockCount == tmpUrl.size()) {
		System.err.println("Waiting on blockCount " + blockCount);
                _wait();
            }
	    else {
	    */
inner:      for(int i=0; i<tmpUrl.size(); i++) {
		ret = null;
		// try {
		Object obj = tmpUrl.elementAt(i);
		ret = MemCache._entryHash.get(obj);
		if(ret != null) { 
		    // System.err.println("Got back blocked result!");

		    try { 
			// System.err.println("A blocked result " + obj);
			Object val = ((MemCacheEntry)ret).val;
			if(val!=null)
			    req.s.put(val);
			else {
			    // System.out.println("HOW DO YOU GET HERE?" + obj);
			    continue inner;
			}
		    } catch (Exception se) {
			System.err.println("In FetchThread");
		    }
		    blockCount--;			
		    tmpUrl.removeElement(obj);

		    cache._add(obj, this);
		    
		    continue outer;
		}
	    }
	    notified = false;	
	}
	System.err.println("All Fetch Done!");
	try { 
	    req.s.close();
	} catch (Exception se) {
	    System.err.println("sourceStream close err");
	    se.printStackTrace();
	}
	System.err.println("SourceStream closed");
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


