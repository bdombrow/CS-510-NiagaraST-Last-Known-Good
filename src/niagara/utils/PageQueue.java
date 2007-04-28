/**********************************************************************
  $Id: PageQueue.java,v 1.6 2007/04/28 22:31:34 jinli Exp $


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


package niagara.utils;

/**
 *  Array-based implementation of a queue of TuplePages for use
 * in streams.
 */

public class PageQueue
{
    // length of the queue; the length passed to the constructor
    // plus 1, 1 slot remains unused when queue is full, so we
    // can tell the difference between full and empty
    private int length;
    private TuplePage[] queue;
    
    // pointers to the head and rear of the queue, respectively
    private int head = 0, tail = 0;
        
    // some queues can grow and never get full - this is for
    // downstream control queues which should never grow large anyway
    private boolean expandable;

    // for debugging
    private String name;
    
    /**
     * This is the constructor for the Queue class
     *
     * @param length the length of this queue; 
     **/
    public PageQueue(int length, String name) {
	this(length, false, name);
    }
    
    /**
     * This is the constructor for the Queue class
     *
     * @param length the length of this queue; 
     **/
    public PageQueue(int len , boolean expandable, String name)
    {
	if(length <= 0)
	    length = 5;
	
	length = len+1;
	queue = new TuplePage[length]; 
	this.expandable = expandable;
	this.name = name;
    }

    /**
     * @return true if the queue is empty; false otherwise
     */
    public boolean isEmpty() {
	return (head == tail);
    }
    

    /**
     * @return true if the queue is full; false otherwise
     */
    public boolean isFull() {
	return ((tail + 1)%length == head);
    }
    

    /**
     * @return the object at the front of the queue; 
     * return null if the queue is empty
     **/
    public TuplePage get() {
	if(!isEmpty()) {
	    head = (head + 1) % length;
	    TuplePage p = queue[head];
	    queue[head] = null;
	    /*if (p == null) {
	    	System.err.println("between head and tail, there is null in page queue");
	    	System.err.println("head ="+head + " tail ="+tail);
	    	System.err.println("the size of page queue :"+length);
	    }*/
	    return p;
	} else
	    return null;
    }
    
    /**
     * @param page The tuple page to be appended to the end of the queue
     *
     **/
    public void put(TuplePage page) {
    	
    if (page == null)
    	System.err.println("put a null value to page queue");
    
	assert !isFull() || expandable : 
	    "KT putting into full un-expandable PageQueue not allowed";
	if(!isFull()) {
	    tail = (tail + 1) % length;
	    queue[tail] = page;
	} else if (expandable) {
		System.out.println("is full tail is " + tail + "head is " + head);
	    if(expandQueue()) {
	       put(page);
            } else {
            	System.err.println("Dropping control flag " +
                        CtrlFlags.name[page.getFlag()] + 
			   " on stream " + name);
            	System.err.println("length of the queue: "+length);
	       // queue to big, refused to expand
	       assert !page.hasTuples() : "KT - help dropping tuples";
	       
	    }
	} 
    }
    
    private boolean expandQueue() {
    	//System.err.println("Expanding queue: current len " + length);
    if(length>=100)
	    return false; // refuse to expand

    //System.err.println("expanding queue");
    //System.err.println("before expanding - head ="+head+" tail= "+tail);
	TuplePage[] newQueue = new TuplePage[length*2];
	
	for (int i = 0; i < length; i++) {
		newQueue[i] = queue[(head+i)%length];
	}
	
	/*for(int i=head; i<length; i++) {
	    newQueue[i-head] = queue[i];
	}
	for(int i = 0; i<=tail; i++) {
		newQueue[i+length] = queue[i];
	}*/
	
	head = 0;
	tail = length-1;
	queue = newQueue;
	length = length*2;
	return true;
	//if(length > 20) {
	//        System.err.println("WARNING: KT queue just expanded to 20 slots!! " + name); 
        // }
    }

    public String toString(){
        
        String retString = new String();
	
        retString += "-----------------------------------------------\n";
        for(int i = 0; i< queue.length; i++){
            if(head == tail){
                retString += "(empty)\n";
                break;
            }
            if(i==head)
                retString += "HEAD "+i+": ";
            else if(i==tail)
                retString += "TAIL "+i+": ";
            else if ((head < tail &&( i< head || i > tail)) ||
                     head > tail && ( i < head && i > tail))
                retString += "open "+i+": ";
            else
                retString += "elem "+i+": ";
            retString += queue[i];
            retString += "\n";
	}
	retString += "-----------------------------------------------\n";
	return retString;
    }
    
    public int getHead() {
    	return head;
    }
    public int getTail() {
    	return tail;
    }
}



