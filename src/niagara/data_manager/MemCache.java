
/**********************************************************************
  $Id: MemCache.java,v 1.2 2001/08/08 21:25:48 tufte Exp $


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

/**
 * Niagra DataManager
 * Replace algorithm for Cache/Disk Management
 * 
 */

import java.util.*;
import java.io.*;
import org.w3c.dom.*;

import niagara.ndom.*;
import niagara.trigger_engine.*;
import niagara.query_engine.*;
import niagara.data_manager.XMLDiff.*;

abstract class MemCache implements DMCache {
    // System wide entryHash, thus _Sharing_ between
    // different rep_alg cache is automatic.
    public static Hashtable _entryHash = new Hashtable();
    public static Mutex     _mutex     = new Mutex();

    // algorithm data
    protected long totalSize; 
    protected long currentSize; 
    protected long highWatermark;
    protected long lowWatermark;

    protected DMCache lowerCache;
    
    abstract void addentry(MemCacheEntry entry);
    abstract void removeentry(Object key);
    abstract void replace();
    
    public static void total_release() {
        _mutex.lock();
        // System.err.println("Releasing MemCache ... size " + _entryHash.size());
        Enumeration en = _entryHash.elements();
        while(en.hasMoreElements()) {
            // System.err.println("Releasing 1 cache entry");
            MemCacheEntry entry = (MemCacheEntry)en.nextElement();
            entry.flush();
        }
        _mutex.unlock();
    }

    /** get TimeStamp of a file or URL */
    public static long getTimeStamp(String ss) {
        String s = CacheUtil.normalizePath(ss); 
        if(CacheUtil.isTrigTmp(s)) {
            MemCacheEntry me = (MemCacheEntry)_entryHash.get(s);
            if(me!=null) return me.getTimeStamp();
        }
        return CacheUtil.getTimeStamp(s);
    }
    
    /** replace old k=>oldval to k=>val */
    public void remap(Object key, Object val) {
        remap(key, val, 0);
    }
    public void remap(Object key, Object val, long vc) {
        // System.err.println("Remapping " + key + " to " + val);
        String k = CacheUtil.normalizePath(key);
        boolean isVec = false;
        if(val == null || val instanceof Vector) {
            isVec = true;
        }
        _mutex.lock();
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(k);
        if(me!=null) {
            // System.err.println(" %%%% remapping in_mem " + k + " to " + val);
            me.setval(val);
            me.setDirty(true);
            if(vc!=0) {
                // System.err.println("%%%%% MemC::remap: setTimeStamp " + vc);
                me.setTimeStamp(vc);
            }
            if(isVec) { 
		System.out.println("Calling me.shrink()");
		me.shrink();
	    }
            _mutex.unlock();
        } else {
            _mutex.unlock();
            // System.err.println(" %%%% remapping a not in_mem " + k);
            if(!isVec) {
                MemCacheEntry mme = new MemCacheEntry(k, val);
                if(vc!=0) mme.setTimeStamp(vc);
                _entryHash.put(k, mme);
                addentry(mme);
            }
            else {
                // This is extremely unlikely to happen since remap is
                // only used in a controlled way by DM.  If this DOES
                // happen, it is expensive -- since we had to preserv
                // the timespan of the Vec file.
                try {
                    // System.err.println("Once Vec should never be here");
                    MemCacheEntry mme = new MemCacheEntry(k, val);
                    mme.setDirty(true);
                    if(vc!=0) mme.setTimeStamp(vc);
                    _entryHash.put(k, mme);
                    addentry(mme);
                } catch(Exception cpe) { 
                    // System.err.println("remap: failed adding a new vec entry to cache.");
                }
            }
        }
    }

    public boolean _add(Object obj, FetchThread fth) {
	String key = CacheUtil.normalizePath(obj);
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
	if(me==null) {
	    System.err.println("$$$$$$$ ERROR ERROR : _add: " + obj);
	    return false;
	}
	
	// System.err.println("In MC::_add: cacheSize " +_entryHash.size());
        if(me._removeWaitingThread(fth)) { 
            // I am the last waiting thread, so I need to put
            // it in _MY_ cache.
	    // System.err.println("Will add entry " + obj);
	    // System.err.println("In MC::_add: cacheSize " +_entryHash.size());
            addentry(me);
	    // System.err.println("Successfully added" + _entryHash.size());
        }
	return true;
    }
        
    public void setLowerCache(DMCache p) {
        lowerCache = p;
    }
    
    /** Cache LookUp routine.  If a file or URL is available in MemCache,
      * it is returned imediately.  Otherwise passed to DiskCache.  If 
      * locally cached in Disk or is local file, it is parsed, and
      * returned.  Otherwise passed to WebCache.
      * @param key, file or URL to fetch
      * @param th,  the fetch thread.  
      * @return Document fetched.  NULL if fetch goes to web.
      */
    public synchronized Object fetch(Object k, Object th) throws CacheFetchException {
        FetchThread fth = (FetchThread)th;
        String key = CacheUtil.normalizePath(k);
        MemCacheEntry ret = (MemCacheEntry)_entryHash.get(key);
	// System.out.println("******** MemCache::Fetch : " + k);
        // boolean cacheMiss = false;
        if(ret==null) { // Cache Miss.
            // System.err.println("MemCache Miss");
            // cacheMiss = true;
            _mutex.lock(); // LOCK mutex befor update _entryHash.
            ret = (MemCacheEntry)_entryHash.get(key);
            if(ret == null) { // Nobody else mess with this key.
                ret = new MemCacheEntry(key, null);
                ret._initWaiting();
                _entryHash.put(key, ret);
                _mutex.unlock();
                Object nret = null;
                try {
                    nret = lowerCache.fetch(key, ret);
                } catch (CacheFetchException cfe) {
                    // Exception in DiskCache.  Use a dummy ...
                    // // System.err.println("Got a ONCE file.");
                    ret.setOnce(1);
                    nret = DOMFactory.newDocument();
                }
                if(nret == null) { // Miss bad.  Goto web and return null
                    // System.err.println("MemCache Miss BAD.  Return NULL");
                    if(fth==null) { // No fetching Thread. 
                        // This is really bad miss.  No fetching 
                        // thread means the fetch is called _WITHIN_
                        // trigger or dm.  They should be only on local
                        // cached files.  In Fact.  They _SHOULD_ call
                        // fetch_reload.
                        try {
                            unpin(key);
                        } catch (Exception upe) {
                        }
                        throw( new CacheFetchException("Pin Remote File"));
                    }
                    ret._addWaitingThread(fth);
                    return null;
                }
                else { // Hijacked locally.  Happily return.
		    // System.err.println("MC:: DISK HIT");
                    _mutex.lock();
                    ret.setval(nret);
                    Element root = ((Document)nret).getDocumentElement();
                    if(root!=null) {
                        String ts = root.getAttribute("TIMESPAN");
                        if(ts!=null && !ts.equals("")) {
                            ret.setTimeSpan(Long.parseLong(ts));
                        }
                    }

                    if(ret.isOnce()) {
			System.out.println("MC:: DISK HIT ONCE FILE");
			_entryHash.put(key, ret);
		    } else {
			// System.err.println("MC:: Test Waiting ");
			boolean testW = ret._notifyWaitingThreads();
			if(!testW) {
			    // System.err.println("MC:: Test Waiting True");
			    this.addentry(ret);
			}
		    }
		    // System.err.print("MC:: UNLOCKING MUTEX");
                    _mutex.unlock();
                    return nret;
                }
            } 
            else { // ret = null in first access, but !=null after mutex.
                Object toret = ret.val;
                if(toret==null) 
                    ret._addWaitingThread(fth);
                _mutex.unlock();
                return toret;
            }
        }
        else { // ret != null in first access.
            Object toret = ret.val;
            if(toret == null) { // OOPS.  Hit on a _not_completed_fetch_
                // System.err.println("Hit a _not_completed_fetch_ " + key);
                _mutex.lock();
                toret = ret.val;
                if(toret==null) ret._addWaitingThread(fth);
                _mutex.unlock();
                return toret;
            }
            
            // System.err.println("%%$$%%^^ CacheHIT " + key);
            // System.err.println("Will return " + getType(key));
            return toret;
        }                
    }
         
    /** This is the _BLOCKING_ version
      * of fetch
      */
    public Object fetch_reload(Object k, Object th) throws CacheFetchException {
        MemCacheEntry me = null;
        String key = CacheUtil.normalizePath(k);
        _mutex.lock();
        me = (MemCacheEntry)_entryHash.get(key);
        if(me!=null) {
            // System.err.println("%%%%%% Cache_Reload. HIT" + k + " on " +
            //        me.val);
            _mutex.unlock();
            return me.val;
        }
        
        me = new MemCacheEntry(key, null);
        _entryHash.put(key, me);
            
        me.addPinCount();
        _mutex.unlock();
        Object v = lowerCache.fetch_reload(key, me);
        if(v==null) {
            // System.err.println("Fetch Reload: got NULL from lower" + key);
            v = DOMFactory.newDocument();
        }
        else {
            Element root = ((Document)v).getDocumentElement();
            if(root!=null) {
                String ts = root.getAttribute("TIMESPAN");
                if(ts!=null && !ts.equals("")) {
                    long tsp = Long.parseLong(ts);
                    me.setTimeSpan(tsp);
                }
            }
        }
        _mutex.lock();
        me.setval(v);
        _mutex.unlock();
        addentry(me);
        me.minusPinCount();
        return me.val;
    }

    public Object fetch_force_reload(Object k) throws CacheFetchException {
        boolean needadd = false;
        MemCacheEntry me = null;
        String key = CacheUtil.normalizePath(k);
        _mutex.lock();
        me = (MemCacheEntry)_entryHash.get(key);
        if(me==null) { 
            me = new MemCacheEntry(key, null);
            _entryHash.put(key, me);
            needadd = true;
        } 
        me.addPinCount();
        _mutex.unlock();
        Object v = lowerCache.fetch_reload(key, me);
        // if(v==null) throw(new CacheFetchException("reload failed"));
        _mutex.lock();
        me.setval(v);
        me.minusPinCount();
        _mutex.unlock();
        if(needadd) addentry(me);
        return me.val;
    }

    /** pin the key=>val pair in Memory.  The pair maybe pined
     * mutiple times. 
     */
    public void pin(Object key) throws CachePinException {
        MemCacheEntry kme = null;
        Object keykey = null;
        if(key instanceof MemCacheEntry) {
            kme = (MemCacheEntry)key;
            keykey = kme.key;
            _mutex.lock();
            MemCacheEntry me = (MemCacheEntry)_entryHash.get(keykey);
            if(me!=null) {
                me.addPinCount();
                if(kme!=null) {
                    me.setval(kme.val);
                    if(kme.isOnce()) me.setOnce(kme.getOnce());
                }
            }
            _mutex.unlock();
        }
        else {
            _mutex.lock();
            MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
            if(me!=null) {
                me.addPinCount();
                _mutex.unlock();
            }
            else {
                _mutex.unlock();
                Object val = null;
                try {
                    val = fetch_reload(key, null);
                } catch (CacheFetchException cfe) {
                    throw(new CachePinException("Pin Null"));
                }
                if(val!=null) pin(new MemCacheEntry(key, val));
            }
        }
    }

    /** Unpin the key->val pair.  The pair is dropped from
     * memory only if the pinCount of MemCacheEntry is 0.
     */
    public void unpin(Object key) throws CacheUnpinException {
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
        if(me==null) 
            throw( new CacheUnpinException("Unpin NULL"));
        int pinc = me.minusPinCount();
        if(pinc==0) {
            if(me.cache==null) {
                _mutex.lock();
                _entryHash.remove(key);
                System.out.println("Unpin remove a entry " + key);
                System.out.println("Now entryHash size " + _entryHash.size());
                _mutex.unlock();
                me.flush();
            }
        }
    }

    public void unpinOnce(Object key) {
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
        if(me==null) return;
        if(!me.isOnce()) return;
        if(me.minusOnce()) { 
	    System.out.print("Trying unpin once file");
            try {
                unpin(key);
            } catch (CacheUnpinException cupe) {
                System.err.println("Unpin once file failed");
            }
        }
    }

    public void setOnceCount(Object key, int onceCount) {
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
        if(me==null) return;
        me.setOnce(onceCount);
    }

    /** Create a Document and add to the Cache.  If create called 
     * on an _EXISTING_ file, the existing doc is returned. 
     */
    public Document createDoc(String s) {
        Document ret = null;
        try {
            ret = (Document)fetch_reload(s, null);
        } catch (Exception fe) {
        }
        return ret;
    }

    /** Modify a doc.  This method is called if somebody holding
     * it want a sync.
     */
    public void modifyDoc(Object key, Object val) {
        remap(key, val);
        //
        /*
           MemCacheEntry ne = (MemCacheEntry)_entryHash.get(key);
           if(ne!=null) {
           ne.setval(val);
           ne.setDirty(true);
           }
           else {
           ne = new MemCacheEntry(key, val);
           ne.setDirty(true);
           addentry(ne);
           }
         */
    }

    public void flushDoc(String file) {
        // System.err.println("Flushing " + file);
        String key = CacheUtil.normalizePath(file);
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
        if(me!=null && (!me.isOnce()) ) me.flush();
    }
    public boolean deleteDoc(String s) {
        MemCacheEntry ne = (MemCacheEntry)_entryHash.get(s);
        if(ne!=null && ne.cache!=null) { 
            ne.cache.removeentry(s);
        }
        _entryHash.remove(s);
        File tmpF = new File(s);
        if(tmpF.exists()) tmpF.delete();
        else return false;
        return true;
    }

    public String getType(Object s) {
        String key = CacheUtil.normalizePath(s);
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(key);
        if(me==null) return(new String("getType: " + s + " not in Mem"));
        return me.getType();
    }
    public void setOnce(Object k) {
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(k);
        if(me!=null) me.setOnce(1);
    }
    public void setTimeSpan(Object k, long tsp) {
        MemCacheEntry me = (MemCacheEntry)_entryHash.get(k);
        if(me!=null) me.setTimeSpan(tsp);
    }
}
