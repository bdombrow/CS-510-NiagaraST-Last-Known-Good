
/**********************************************************************
  $Id: CacheManager.java,v 1.8 2003/03/08 02:21:39 vpapad Exp $


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
import org.w3c.dom.*;
import java.util.*;
import niagara.utils.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 *  Initial implementation of data manager no memory management
 *  essentially managing two directories
 *  @version 1
 */
public class CacheManager {
    private String cacheDir;
    private DiskCache diskCache;
    private MemCache  memCache;
    private Vector otherCacheVec;

    private long vClock;
    // vClock(virtual Clock)  is used to syncronize with EventDector.  
    // CacheManager and ED are 2 threads, so they see different real
    // clock.  CacheManager use virtual clock set by ED to avoid 
    // confusion.  Idea is simple, whenever ED communicate with DM,
    // it pass ED's clock.  DM adjust vClock accordingly.
        
    public CacheManager(String path) {
        cacheDir = path;
        // System.err.println("Using " + cacheDir + " as Cache Dir");
        diskCache = new DiskCache(cacheDir);
        memCache  = new MemCacheFIFO(20, 10, 15);
        memCache.setLowerCache(diskCache);
        otherCacheVec = new Vector();
        vClock = 0;
    }

    public MemCache createCache(int rep, int totalS, int lw, int hw) {
        // right now I ignore replace alg.  Just FIFO
        MemCache mc = new MemCacheFIFO(totalS, lw, hw);
        mc.setLowerCache(diskCache);
        otherCacheVec.addElement(mc);
        return mc;
    }
    
    public void releaseCache(MemCache mc) {
        try {
            mc.release();
        } catch (CacheReleaseException cre) {
            cre.printStackTrace();
        }
        otherCacheVec.removeElement(mc);
    }
    
    public long getVClock() {
        return vClock;
    }
    public void setVClock(long now) {
        if(vClock>=now) { 
            // System.err.println("vClock error");
            // vClock++;
        }
        else vClock = now;
    }
    
    public boolean isModified(String s, long from, long to) {
	System.out.println("CM::isModified: Detecting " + s);
	System.out.println("CM::isModified: From " + from + " To " + to);
        setVClock(to);
        long tsp = MemCache.getTimeStamp(s);
	System.out.println("CM::fileTimeStamp: " + tsp);
        if(tsp >= from || tsp == 0) return true;
        else return false;
    }
    
            
    public synchronized boolean getDocuments(Vector xmlURLList, 
					     regExp pathExpr,   
					     SinkTupleStream stream) 
    throws ShutdownException {
        int numFetches = xmlURLList.size();
        // System.err.println("Getting " + numFetches + " docs");
        // Put in the fetchInfo object in service queue
        //
        String tmps = (String)xmlURLList.elementAt(0);
	if(CacheUtil.isAccumFile(tmps)) {
	    getAccumFile(tmps, stream);
	} else if(CacheUtil.isOrdinary(tmps)) {
            // System.err.println("CacheM: Trying get Normal file " + tmps);
            Vector dottedPaths = null;   // DMUtil.convertPath(pathExpr);
            FetchRequest newRequest = new FetchRequest(stream, 
                    xmlURLList, dottedPaths);
            FetchThread fth = new FetchThread(newRequest, memCache);
	} else {
            // XXX vpapad: we should never get here
            // what the !@#!@# does isOrdinary() do?
            assert false;
            // System.err.println("CacheM. getDoc " + tmps);
            // getTrigDocument(tmps, pathExpr, stream);
        }
        return true;
    }


    /** Function to retrieve the most current document associated
     * with an Accumulate File. Finds the document using the global
     * Accumulate File Directory
     *
     * @param afName The name of the accum file
     * @param s Stream to put result into.
     *
     */
    private void getAccumFile(String afName, 
			      SinkTupleStream outputStream) 
	throws ShutdownException {
	try {
	    Document accumDoc = (Document)DataManager.AccumFileDir.get(afName);
	    outputStream.put(accumDoc);
	    outputStream.endOfStream();
	} catch (java.lang.InterruptedException e) {
	    throw new PEException("What happened?!" + e.getMessage());
	}
	return;
    }

    public void shutdown() {
        // System.err.println("shutting down cache manager ...");
        try {
            MemCache.total_release();
            diskCache.release();
        } catch (CacheReleaseException cre) {
            // System.err.println("Cannot Gracefully shutdown CacheManager");
        }
    }
}
