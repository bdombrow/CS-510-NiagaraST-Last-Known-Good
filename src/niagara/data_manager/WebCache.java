
/**********************************************************************
  $Id: WebCache.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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
  */

/* WebCache is not cache at all.  Its only functionality
 * is going out to web and fetch back a Url
 */

import java.io.*;
import java.net.*;

import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.data_manager.XMLDiff.*;

class WebCache implements DMCache {

    UrlQueue urlq;
    UrlFetchThread[] urlth;
    int numThr;
    DMCache higherCache;

    /** Constructor.  
      * @param nthr, number of fetch threads
      * @param hc, High Level Cache
      */
    WebCache(int nthr, DMCache hc) {
        numThr = nthr;
        higherCache = hc;
        urlq = new UrlQueue(100); // at most 100 outstanding request
        urlth = new UrlFetchThread[nthr];
        for(int i=0; i<nthr; i++) {
            urlth[i] = new UrlFetchThread(urlq, hc);
        }
    }
    
    /** fetch a file from web to local disk.
      * @param key,  local file name
      * @param m, a MemCacheEntry.  It is used to hold waiting 
      *        threads on this fetch.
      */
    public Object fetch(Object key, Object m) throws CacheFetchException {
        MemCacheEntry me = (MemCacheEntry)m;
        // System.err.println("WebCache Fetching ... "+key);
        UrlQueueElement req = new UrlQueueElement((String)key,me);
        urlq.addUrl(req);
        return null;
    }
    
    /** fetch upto date copy.  Same as fetch in WebCache 
      */
    public Object fetch_reload(Object key, Object me) throws 
        CacheFetchException 
    {
        return fetch(key, me);
    }

    /** release a webCache.  Just shutdown all fetch threads */
    public void release() throws CacheReleaseException {
        boolean needThrow = false;
        for(int i=0; i<urlth.length; i++) {
            try {   
                urlth[i].interrupt();
            } catch (Exception ie) {
                needThrow = true;
            }
        }
        if(needThrow) 
            throw(new CacheReleaseException("Shutdown UrlFetchThread failed"));
    }

    /** Should never be called on WebCache */
    public void pin(Object key) throws CachePinException {
        throw(new CachePinException("WebCache cannot pin"));
    }

    /** Should never be called on WebCache */
    public void unpin(Object key) throws CacheUnpinException {
        throw(new CacheUnpinException("WebCache cannot unpin"));
    }
}
