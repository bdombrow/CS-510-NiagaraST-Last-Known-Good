
/**********************************************************************
  $Id: MemCacheEntry.java,v 1.3 2003/03/08 01:01:53 vpapad Exp $


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

/** Niagra. DataManager. MemCacheEntry
  */
import java.util.*;
import org.w3c.dom.*;

class MemCacheEntry {
    Object key; // usually should be a string
    Object val; // usually a Document

    // stats
    long size;
    boolean dirty;
    int once; // 
    int age;
    int flag;
    volatile int pinCount;
    MemCache cache;

    private long timespan;
    private volatile long timeStamp;

    private Vector waitingThreads;

    // relationship to other entry
    MemCacheEntry prev;
    MemCacheEntry next;

    MemCacheEntry(Object key, Object val) {
        this.key = key;
        this.val = val;
        this.once = 0;
        this.pinCount = 0;
        dirty = false;
        this.size = 1;
        waitingThreads = null;
        cache = null;
        timespan = 0;
    }
    public void setTimeSpan(long ts) {
        timespan = ts;
    }
    public long getTimeStamp() {
        return timeStamp;
    }
    public void setTimeStamp(long ts) {
        // System.err.println("MemCaEntry.setTS: Set TimeStamp: " + key + " " + ts);
        timeStamp = ts;
    }
    public void shrink() {
        System.out.println("Shrinking .... ");
        if(val == null) {
            // System.err.println("Trying to shink null vec");
            return;
        }
        if(val instanceof Vector) {
            Vector v = (Vector)val;
            // System.err.println("before shrink size " + v.size());
            dirty = CacheUtil.shrinkVec(timespan, (Vector)val) || dirty;
            // System.err.println("After shrink size " + v.size());
        }
    }
    public void flush() {
        if(dirty) {
            // System.err.println("Flushing a MemCacheEntry");
            if(key instanceof String && val instanceof Document)
                CacheUtil.flushXML((String)key, (Document)val);
            else if(key instanceof String && val instanceof Vector) {
                shrink();
                CacheUtil.flushVec((String)key, timespan, (Vector)val);
            }
        }
    }
    public void setOnce(int o) {
        once = o;
    }
    public boolean isOnce() {
        return(once!=0);
    }
    public boolean minusOnce() {
        once--;
        if(once==0) return true;
        return false;
    }
    public int getOnce() {
        return once;
    }
    public String getType() {
        if(val == null) return(new String("Type is NULL"));
        if(val instanceof Document) return(new String("Type is Doc"));
        if(val instanceof Vector) {
            Vector v = (Vector) val;
            return(new String("Type is Vec: size " + v.size()));
        }
        return(new String("Unsupported Type in MemCacheEntry"));
    }
    public void setDirty(boolean d) {
        dirty = d;
        if(d) timeStamp = System.currentTimeMillis();
    }
    public boolean isDirty() {
        return dirty;
    }
    public synchronized void setval(Object v) {
        // System.err.println("Set Val of " + key);
        // System.err.println("Val is " + v);
        val = v;
    }
    public synchronized int getPinCount() {
        return pinCount;
    }
    public synchronized void setPinCount(int c) {
        pinCount = c;
    }
    public synchronized void addPinCount() {
        pinCount++;
    }
    public synchronized int minusPinCount() {
        if(pinCount>0) pinCount--;
        return pinCount;
    }
    public synchronized void setCache(MemCache toset) {
        cache = toset;
    }
    public synchronized boolean isCacheNull() {
        return(cache==null);
    }
    public void _initWaiting() {
        waitingThreads = new Vector();
    }
    public synchronized void _addWaitingThread(FetchThread fth) {
        waitingThreads.addElement(fth);
    }
    public synchronized boolean _removeWaitingThread(FetchThread fth) {
        waitingThreads.removeElement(fth);
        if(waitingThreads.size()==0) return true;
        return false;
    }
    public synchronized boolean _notifyWaitingThreads() {
        if(waitingThreads.size()==0) return false;
        for(int i=0; i<waitingThreads.size(); i++) {
            ((FetchThread)waitingThreads.elementAt(i))._notify();
        }
        return true;
    }
}

