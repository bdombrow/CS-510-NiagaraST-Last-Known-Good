
/**********************************************************************
  $Id: CacheManager.java,v 1.7 2003/03/08 01:01:53 vpapad Exp $


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
import com.ibm.xml.parser.*;
import java.util.*;
import java.io.File;
import java.io.IOException;
import niagara.ndom.*;
import niagara.utils.*;
import niagara.data_manager.XMLDiff.*;
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
    

    public void setFileSpan(String s, long span, boolean isTrigTmp) {
        String tmp = null;
        boolean inMemVec = false;
        if(isTrigTmp) tmp = s; 
        else if(CacheUtil.isUrl(s)) 
            tmp = diskCache.urlToFile(s)+"_DIFF_.xml";
        else 
            tmp = diskCache.pathToFile(s)+"_DIFF_.xml";
        Document doc = null;
        try {
            if(span==0) {
                File tmpF = new File(tmp);
                if(tmpF.exists()) tmpF.delete();
                memCache.setOnce(s);
                return;
            }
            else doc = (Document)memCache.fetch_reload(tmp, null);
	} catch(CacheFetchException fe) {
            memCache.setTimeSpan(s, span);
            inMemVec = true;
	}
        if(inMemVec) return;
        
        // System.err.println("**** Temp/Diff fetch done! *** " + tmp);
        if(doc.getDocumentElement()==null) {
            // System.err.println(" ########## Got a Dummy Doc ###### ");
            Element root = doc.createElement("ROOT");
            doc.appendChild(root);
            root.setAttribute("DIRTY", "TRUE");
            root.setAttribute("LASTSTAMP", ""+getVClock());
        }
        // Here I add a 10 sec delay to the span.  Hopefully, if
        // QE can schedule a query within 10 sec of ED asks for it,
        // this should be fine.  A precise span could be maintained,
        // but simply TOO EXPENSIVE.
        CacheUtil.setFileSpan(doc, span + 10000);
        memCache.remap(tmp, doc);
    }

    public void storeTuples(String file, Vector v) {
        // StreamTupleElement ste = (StreamTupleElement)v.elementAt(0);
        // System.out.println("StoreTuple: ste size " + ste.size());
        System.out.println("CM.storeTuple: " + file + ": size " + v.size());
        Vector v2 = scanTuples(file, -1, 0);
        if(v2==null) {
            System.out.println("StoreTuple: Got null from scanTuple");
            v2 = v;
            // XXX vpapad TODO: this code has to be cleaned
            // commenting out and asserting out for now
            assert false;
            
//            int onceCount = eventDetector.getTriggerCount(file);
//            memCache.setOnceCount(file, onceCount);
        }
        else {
            System.out.println("CM.storeTuple: Scanned v2 size " + v2.size());
            if(v.size()==0) return;
            for(int i=0; i<v.size(); i++) {
                v2.addElement(v.elementAt(i));
            }
        }

        long vc = getVClock();
        CacheUtil.setTimeStamp(v2, vc);
        // System.err.println("storeT. remapping " + file + " to " + v2);
        memCache.remap(file, v2, vc);
        // System.out.println("@@@@@ After store remap, size " + v2.size());
        // System.out.println("@@@@@ I am pressing ED Button on " + file);
        // XXX vpapad TODO: this code has to be cleaned
        // commenting out and asserting off now
        assert false;
//        eventDetector.setFileLastModified(file,
//                new Date(vc));
        // System.err.println("Return from storeTuple " + file);
    }
    
    public Vector scanTuples(String file, long from, long to) {
        // System.err.println("Test Type " + memCache.getType(file));
        Object obj = null;
        Vector v = null;
        try {
            // System.err.println("scanTuple Fetching " + file);
            obj = memCache.fetch(file, null);
            // System.err.println("Fetching done");
        } catch (CacheFetchException cfe) {
            System.out.println("CacheM scanTuples fetch error");
        }
        if(obj instanceof Document) {
            // System.err.println("scanTuple fetched doc " + file);
            v = CacheUtil.tmpDocToVec((Document)obj);
            // System.err.println("Remap it to Vec " + v);
            memCache.remap(file, v);
        }
        else { 
            // System.err.println("scanTuple: fetched vec from memCache");
            v = (Vector)obj;
        }
        if(from == -1)  {
            // System.err.println("Return from scanT to storeT");
            return v;
        }
        else {
            // System.err.println("Scan Tuple: return to QE");
            // System.out.println("Before GetVecSince, size " + v.size());
            Vector ret = CacheUtil.getVecSince(v, from, to); 
            System.out.println("GetVecSince return " + ret.size() + "%%%");
            memCache.unpinOnce(file);
            // System.err.println("Will pushup tuples ");
            return ret;
        }
    }
            
    public Document createDoc(String s) {
        return memCache.createDoc(s);
    }

    public void modifyDoc(Object key, Object val) {
        memCache.modifyDoc(key, val); 
    }
    public void flushDoc(String file) {
        memCache.flushDoc(file);
    }

    public boolean pinDoc(String s) {
        boolean ret = true;
        try {
            memCache.pin(s);
        } catch (CachePinException cpe) {
            ret = false;
        }
        return ret;
    }

    public boolean unpinDoc(String s) {
        boolean ret = true;
        try {
            memCache.unpin(s);
        } catch (CacheUnpinException cupe) {
            ret = false;
        }
        return ret;
    }

    public void deleteDoc(String s) {
        memCache.deleteDoc(s);
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
