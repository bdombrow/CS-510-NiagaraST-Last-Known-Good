
/**********************************************************************
  $Id: CacheManager.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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
import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.util.*;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import niagara.trigger_engine.*;
import niagara.query_engine.*;
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
    private EventDetector eventDetector;

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

    public void setEventDetector(EventDetector ed) {
        eventDetector = ed;
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
    
    /** Monitor a trigger file.  2 file are created, one to
      * save the original file, one to record diff. 
      */
    public boolean Monitor(String s, boolean isTrigTmp) throws IOException {
        String tmp = null;
        File tmpF = null;
        // System.err.println(" ++++++++ Monitor " + s );
        if(!isTrigTmp) {
            /* save a copy of the file in local directory */
            // System.err.println("Monitoring usurall file " + s);
            if(CacheUtil.isUrl(s)) {
                tmp = diskCache.urlToFile(s);
                tmpF = new File(tmp);
                if(tmpF.exists()) return CacheUtil.isPush(tmp);
                System.out.println("Fetching " + s + " to " + tmp);
                CacheUtil.fetchUrl(s, tmp);
            } 
            else {
                tmp = diskCache.pathToFile(s);
                tmpF = new File(tmp);
                if(tmpF.exists()) return CacheUtil.isPush(tmp);
                // System.err.println("Fetching " + s + " to " + tmp);
                CacheUtil.fetchLocal(s, tmp);
            } 
        }
        else return true;
        return CacheUtil.isPush(tmp);
    }

    /** UnMonitor a file.  The original and diff file are deleted. */
    public void unMonitor(String s, boolean isTrigTmp) {
        String tmp = null;
        if(!isTrigTmp) {
            if(CacheUtil.isUrl(s)) tmp = diskCache.urlToFile(s);
            else tmp = diskCache.pathToFile(s);

            // System.err.println("Deleting monitored file " + tmp);
            memCache.deleteDoc(tmp);
            memCache.deleteDoc(tmp+"_DIFF_.xml");
        } 
        else {
            // System.err.println("Deleting monitored trigger tmp " + tmp);
            memCache.deleteDoc(tmp);
        }
    }

    public void setFileSpan(String s, long span, boolean isTrigTmp) {
        String tmp = null;
        boolean inMemVec = false;
        if(isTrigTmp) tmp = s; 
        else if(CacheUtil.isUrl(s)) 
            tmp = diskCache.urlToFile(s)+"_DIFF_.xml";
        else 
            tmp = diskCache.pathToFile(s)+"_DIFF_.xml";
        TXDocument doc = null;
        try {
            if(span==0) {
                File tmpF = new File(tmp);
                if(tmpF.exists()) tmpF.delete();
                memCache.setOnce(s);
                return;
            }
            else doc = (TXDocument)memCache.fetch_reload(tmp, null);
        } catch(Exception fe) {
            memCache.setTimeSpan(s, span);
            inMemVec = true;
        }
        if(inMemVec) return;
        
        // System.err.println("**** Temp/Diff fetch done! *** " + tmp);
        if(doc.getDocumentElement()==null) {
            // System.err.println(" ########## Got a Dummy Doc ###### ");
            TXElement root = new TXElement("ROOT");
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
            int onceCount = eventDetector.getTriggerCount(file);
            memCache.setOnceCount(file, onceCount);
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
        eventDetector.setFileLastModified(file,
                new Date(vc));
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
            
    /** scan part of file inside an interval */
    public Document getDiff(String file, long since, long to) {

        Document resDoc = getDiff(file, to);
        Document ddoc = CacheUtil.getQEDiffDoc(resDoc, since, to);

        return ddoc;
    }

    /** Generate new Diffs */
    private Document getDiff(String s, long tsp) {

        String tmp = null;
        if(CacheUtil.isUrl(s)) tmp = diskCache.urlToFile(s);
        else tmp = diskCache.pathToFile(s);
        TXDocument resDoc = null;

        try {
            resDoc = (TXDocument)memCache.fetch(tmp+"_DIFF_.xml", null);
        } catch ( Exception fe) {
            // System.err.println("Fetch Error, in GetDiff resDoc");
        }
        if(resDoc.getDocumentElement()==null) {
            // System.err.println("====== %%%%%% Dummy resDoc");
            TXElement tmpEle = new TXElement("ROOT");
            tmpEle.setAttribute("TIMESPAN", "0");
            tmpEle.setAttribute("LASTSTAMP", "0");
            tmpEle.setAttribute("DIRTY", "FALSE");
            resDoc.appendChild(tmpEle);
        }

        Diff(tmp, s, resDoc, tsp);

        return resDoc;
    }

    private synchronized void Diff(String s1, String s2, TXDocument resDoc, long tsp) {
	
        long ts1 = CacheUtil.getTimeStamp(s1);
        long ts2 = CacheUtil.getTimeStamp(s2);
        if(ts2==0) ts2 = getVClock();
        System.out.println("Diffing " + s1 + " $$$ " + s2);
        System.out.println("Diffing Time stamp " + ts1 + " $$$ " + ts2);
        if(ts1<ts2) {
	     System.err.println("%%% Checking time stamp ...  needs diff");
	    // System.err.println("%%% This should only appear once!");
	    File tmpF = new File(s1);
	    if(!tmpF.exists()) {
		try {
		TXDocument tmpDoc = CUtil.parseXML(s2);
		Element resRoot = resDoc.getDocumentElement();
		Element tmpRoot = tmpDoc.getDocumentElement();
		NodeList nl = tmpRoot.getChildNodes();
		for(int i=0; i<nl.getLength(); i++) {
		    Node n = nl.item(i);
		    if(n instanceof TXElement) {
			Element eleins = resDoc.createElement("Insert");
			eleins.setAttribute("TIMESTAMP", ""+getVClock());
			// Xpointer = eleins.makeXPointer();
			// eleins.setAttribute("POSITION", p.toString());
			Child nchild = (Child)n.cloneNode(true);
			eleins.appendChild(nchild);
			resRoot.appendChild(eleins);
		    }
		}
		resRoot.setAttribute("DIRTY", "TRUE");
		if(CacheUtil.isUrl(s2))
		    CacheUtil.fetchUrl(s2, s1);
		else
		    CacheUtil.fetchLocal(s2,s1);
		} catch (Exception e) {
		    e.printStackTrace();
		    System.err.print("Illegal XML DOC!");
		}
	    }
            TXDocument doc1 = null;
            TXDocument doc2 = null;
            try {
                doc1 = (TXDocument)memCache.fetch_reload(s1, null);
                // doc2 = (TXDocument)memCache.fetch_reload(s2, null);
                // doc1 = (TXDocument) CUtil.parseXML(s1);
                doc2 = CUtil.parseXML(s2);
		if(doc2.getDocumentElement()==null)
		    System.out.println("BAD DOCUMENT !!! ");
            } catch (Exception cfe) {
                // System.err.println("Error is getting Docs");
                // System.err.println(s1 + " $$ " + s2);
                return;
            }

            try { 
                XMLDiff.getDiffDoc((TXDocument)doc1.cloneNode(true), 
                        (TXDocument)doc2.cloneNode(true), resDoc,
                        tsp);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Diffing get exception: ");
            }
                
	    try { 
		Monitor(s2, false); // fetch new copy.
	    } catch (IOException ioe) {
		ioe.printStackTrace();
	    }
            memCache.remap(s1, doc2);
            memCache.remap(s2, doc2);
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
            SourceStream stream) 
    {
        int numFetches = xmlURLList.size();
        // System.err.println("Getting " + numFetches + " docs");
        // Put in the fetchInfo object in service queue
        //
        String tmps = (String)xmlURLList.elementAt(0);
        if(CacheUtil.isOrdinary(tmps)) {
            // System.err.println("CacheM: Trying get Normal file " + tmps);
            Vector dottedPaths = null;   // DMUtil.convertPath(pathExpr);
            FetchRequest newRequest = new FetchRequest(stream, 
                    xmlURLList, dottedPaths);
            FetchThread fth = new FetchThread(newRequest, memCache);
        }
        else {
            // System.err.println("CacheM. getDoc " + tmps);
            getTrigDocument(tmps, pathExpr, stream);
        }
        return true;
    }

    private void getTrigDocument(String tmps, 
            regExp pathExpr, SourceStream s) {
        int id = CacheUtil.tupleGetTrigId(tmps);
        String fn = CacheUtil.tupleGetTrigFile(tmps);

        Vector vf =  eventDetector.getLastFireTime(fn, id);
        
        long from = ((Long)vf.elementAt(0)).longValue();
        long to = ((Long)vf.elementAt(1)).longValue();

	System.out.println("CM::getTrigDoc: from " + from + " to " + to);

	/* EXTREME HACK!!! EXTREME UGLY HACK !!! */
	if(from==to) from--;
	/* This is the ED problem.  ED somehow pass from==to sometime
	   thus lost result to upper layer trigger.  This solves it,
	   but it _REALLY_ should be ED do some bug fix.  Will harass Yuan
	   about this.
	   */
        setVClock(to);
        boolean isVec = CacheUtil.isTrigTmp(fn);
        try {
            if(isVec) {
                Vector vec = scanTuples(fn, from, to);
		System.out.println("::::: CM:: Pushing up " + vec.size() + " "+s);
                for(int i=0; i<vec.size(); i++) {
		    
                    s.steput((StreamTupleElement)vec.elementAt(i));
                }
            }
            else {
                Document doc = getDiff(fn, from, to);
                s.put(doc);
            }
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
