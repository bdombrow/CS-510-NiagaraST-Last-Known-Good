
/**********************************************************************
  $Id: PageStream.java,v 1.1 2002/04/29 19:54:57 tufte Exp $


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
 *
 * PageStream is the bottom-level class for communicating between 
 * operators. It passes TuplePages between operators. Right now
 * it is implemented as an in-memory queue, although that could
 * be changed.
 *
 * IMPORTANT: functions in this class are divided between functions
 * for use by SourceTupleStreams and SinkTupleStreams. SourceTupleStream
 * functions have Source in their name, SinkTupleStream functions have
 * Sink in their name.
 *
 * @version 1.0
 *
 *
 * @see PhysicalOperator
 */

import java.util.LinkedList;

// Note on removal of upStreamContrlQueue and priority put
// instead of priority put, we have  PageStream check
// the incoming pages to check for any "priority flags"
// and set a flag in PageStream to indicate that a priority
// ctrl flag has been received (currently the only priority
// flag is shutdown)
// this works to propagate a priority element in all cases
// except when queue is full, but I think we can live with that.
// we waste at most one page of processing
// wait priority propagation could be done by simply setting
// a flag on page stream... but this won't work if we move
// to real streams...


public class PageStream {

    // Buffer for propagating tuples and control elements upstream
    private PageQueue upStreamQueue;
    
    // Buffer for propagating control information down stream
    // Note, think I always need to be able to put in the downstream
    // queue without blocking - so make this infinitely expanding (that
    // won't happen of course)
    private PageQueue downStreamQueue; 

    // eos indicates end of stream received from downstream (source, 
    // tuple producer) operator
    private boolean eos;

    // shutdown page is not null if a shutdown control page has been
    // received either from upstream or downstream, shutdown indicates
    // client shutdown request or operator error
    private boolean shutdown;

    private final int STREAM_CAPACITY = 5;

    // keep extra pages around that can be reused 
    private TuplePage[] dataPageBuffer;
    private TuplePage extraCtrlPage;

    // for debugging identification
    private String name;

    //private int existingDataPagesUsed;
    //private int dataPagesAllocd;
    //private int existingCtrlPagesUsed;
    //private int ctrlPagesAllocd;
    //private int notifiedConsumer;
    //private int notifiedProducer;
    //private int notifiedOnCtrl;

    /**
     *  Constructor
     */
    public PageStream(String name) {
	// downstream queue is expandable...
	upStreamQueue = new PageQueue(STREAM_CAPACITY, false);
	downStreamQueue = new PageQueue(STREAM_CAPACITY, true);
	dataPageBuffer = new TuplePage[STREAM_CAPACITY];
	extraCtrlPage = null;
	eos = false;
	shutdown = false;
	this.name = name;
	//existingDataPagesUsed = 0;
	//dataPagesAllocd = 0;
	//existingCtrlPagesUsed = 0;
	//ctrlPagesAllocd = 0;
	//notifiedConsumer = 0;
	//notifiedProducer = 0;
	//notifiedOnCtrl = 0;
    }  

    // ----------------------------------------------------------------------
    // FUNCTIONS FOR USE BY SourceTupleStream ONLY
    // ----------------------------------------------------------------------

    /**
     * Get a page from the source operator (upstreamQueue). This
     * function should only be called by the consumer.
     *
     * If no page is received from source within timeout milliseconds,
     * function times out and null is returned. A timeout of 0,
     * causes the time to be ignored and function will block until
     * page received from source.
     *
     * EOS is indicated by a flag on the page.
     *
     * @param timeout A timeout period in milliseconds
     *
     * @return Page from the source (upstreamQueue), page may have
     *          tuples, a control flag, or both, null if timed out
     *
     */
    // to be called by SourceTupleStream ONLY
    public synchronized TuplePage getPageFromSource (int timeout) 
	throws java.lang.InterruptedException, ShutdownException {

	if(shutdown) 
	    throw new ShutdownException();

	// note, get is allowed after end of stream

	// If the buffer is empty, wait the timeout period, if I
	// wake up and the buffer is still empty, means I timed out 

	// Note - only one consumer on this stream. Notifies come
	// from two possibilites - consumer notifies on downstreamQueue
	// or producer notifies on upstreamQueue, since consumer (which
	// is the only one to call this function) can't notify when
	// sleeping and producer notifies only on upstreamQueue,
	// if we wake up from wait one of two things has happened
	// 1) timeout
	// 2) producer notified on upStreamQueue.
	// we can test which of these two situations we are in by
	// checking if upStreamQueue is empty - notice that since
	// there are no other consumers on this queue, no one else
	// could have taken elements out of this queue before consumer
	// got to run
	
	if(upStreamQueue.isEmpty()) {
	    wait(timeout);
	    if(upStreamQueue.isEmpty()) {
		// I timed out...
		return null;
	    }
	    // else must be something in queue, go on
	} 

	// we have something in the queue...
	// don't check for flags because 1) if shutdown flag, will
	// already have been checked and we shouldn't get here,
	// 2) other flags appear logically after all tuples in page
	boolean notifyProducer = upStreamQueue.isFull();
	TuplePage ret = upStreamQueue.get();

	// KT - DEL after functions - just for checking
	if(ret.getFlag() == CtrlFlags.SHUTDOWN) {
	    throw new PEException("KT shouldn't get here");
	}

	if(notifyProducer) {
	    //notifiedProducer++;
	    notify();
	}

	return ret;
    }

    /**
     * Put a controlPage to the source (downstream queue). This
     * function is non-blocking because the downstream queue is
     * "infinitely" expandable, although it should never grow big
     * at all. 
     *
     * @param controlPage the control page to be sent
     *
     */
    // To be called by SourceTupleStream ONLY
    public synchronized int putCtrlMsgToSource(int ctrlMsgId) 
	throws ShutdownException {

	if(eos)
	    return CtrlFlags.EOS;
	if(shutdown) 
	    throw new ShutdownException();

	// do SHUTDOWN check on put to make propagation of SHUTDOWN
	// as fast as possible
	if(ctrlMsgId == CtrlFlags.SHUTDOWN) 
	    shutdown = true;

	boolean notify = downStreamQueue.isEmpty();

	// Add the control element to the end of the down stream control 
	// buffer
	downStreamQueue.put(getCtrlPage(ctrlMsgId));
	if(notify) {
	    //notifiedOnCtrl++;
	    notify();
	}
	return CtrlFlags.NULLFLAG; // indicates success
    }

    // to be called by SourceTupleStream ONLY
    public synchronized boolean shutdownReceived() {
	return shutdown;
    }

    // to be called by SourceTupleStream ONLY
    public synchronized void returnTuplePage(TuplePage returnPage) {
	for(int i = 0; i<STREAM_CAPACITY; i++) {
	    if(dataPageBuffer[i] == null) {
		dataPageBuffer[i] = returnPage;
		return;
	    }
	}
	// if buffer is full, toss the return page
	return;
    }

    // ----------------------------------------------------------------------
    // FUNCTIONS FOR USE BY SinkTupleStream ONLY
    // ---------------------------------------------------------------------- 
    /**
     * This function returns a control flag sent from the sink
     * operator (the operator "above" this operator in the query tree)
     * a.k.a. a page from the downStreamQueue, if any exists.
     * Otherwise, it returns NULLFLAG. This function is non-blocking.
     *
     * @return The control flag received from the sink message, NULLFLAG
     *           if no control element received.
     *
     */
    // To be called by SinkTupleStream ONLY
    public synchronized int getCtrlMsgFromSink() throws ShutdownException {
	// shutdown takes priority over all other messages
	if(shutdown)
	    throw new ShutdownException();

	// Check for end of stream and raise exception if necessary
	if (eos) {
	    throw new PEException("KT Reading after end of stream");
	}
	
	// Get first element, if any, in the down stream buffer
	if (downStreamQueue.isEmpty()) {
	    return CtrlFlags.NULLFLAG;
	} else {
	    // no notification, no one ever blocks on downStreamQueue
	    TuplePage ctrlPage = downStreamQueue.get();
	    int retVal = ctrlPage.getFlag();
	    returnCtrlPage(ctrlPage); // make page avail for reuse
	    return retVal;
	}
    }

    /**
     * Put a page to the Sink operator - a.k.a. put a page in the
     * upstreamQueue.
     *
     * This function checks for control pages from the sink. If there
     * is a control page(message) from the sink, the tuplePage is not
     * put in the stream and the control page is returned.
     * This function blocks until either the output element can be put in
     * the upStreamQueue (to Sink) or a control page is read from the
     * downStreamQueue (from Sink) (this is our flow control mechanism)
     * 
     * @param page The page to be sent to the Sink
     *
     * @return the control flag, NULLFLAG if put was successful
     *
     */
    // to be called by SinkTupleStream ONLY
    public synchronized int putPageToSink(TuplePage page)
	throws java.lang.InterruptedException, ShutdownException {

	// shutdown takes priority over everything
	if(shutdown)
	    throw new ShutdownException();

	if (eos) 
	    throw new PEException("KT Writing after end of stream");

	// Wait until either the up stream tuple buffer is not full 
	// (so that the outputElement can be put) or the down stream 
	// control buffer is not empty (so that a control element can 
	// be returned).
	while (upStreamQueue.isFull() &&
	       downStreamQueue.isEmpty()) {
		wait();
	}

	// If there is a control element in the down stream buffer, 
	// then return that
	if (!downStreamQueue.isEmpty()) {
	    TuplePage ctrlPage = downStreamQueue.get();
	    int ctrlFlag = ctrlPage.getFlag();
	    returnCtrlPage(ctrlPage);
	    return ctrlFlag;
	} else {
	    // do SHUTDOWN check on put to make propagation of SHUTDOWN
	    // as fast as possible - you may find it strange to
	    // check shutdown in put - this check is done not for
	    // the producer, but for the consumer - note that this shutdown
	    // message is just arriving into the consumers
	    // input stream
	    if(page.getFlag() == CtrlFlags.SHUTDOWN) 
		shutdown = true; 

	    // There must be an open spot in the upstream Queue, so put
	    // the page there.
	    boolean notifyConsumer = upStreamQueue.isEmpty();
	    upStreamQueue.put(page);

	    if(notifyConsumer) {
		//notifiedConsumer++;
		notify();
	    }
	    return CtrlFlags.NULLFLAG; // indicates successful put
	}	
    }

    /**
     * This function closes a stream so that no further upward or downward
     * communication (other than get) is possible. This function blocks
     * until there is space available in the upstreamQueue.
     *
     */
    // To be called by SinkTupleStream ONLY
    public synchronized void endOfStream() {
	// If the stream was previously closed, throw an exception
	if (eos) 
	    throw new PEException("KT end of stream received twice");
	eos = true;
	//System.out.println("Existing Data Pages Used " +existingDataPagesUsed);
	//System.out.println("Data Pages Allocd " + dataPagesAllocd);
	//System.out.println("Existing Ctrl Pages Used " +existingCtrlPagesUsed);
	//System.out.println("Ctrl Pages Used " + ctrlPagesAllocd);
	//System.out.println(name);
	//System.out.println("notified consumer " + notifiedConsumer);
	//System.out.println("notified producer " + notifiedProducer);
	//System.out.println("notified on control " + notifiedOnCtrl);
	//System.out.println();
    }

    // to be called by SinkTupleStream ONLY
    public synchronized TuplePage getTuplePage() {
	TuplePage ret;
	for(int i = 0; i<STREAM_CAPACITY; i++) {
	    if(dataPageBuffer[i] != null) {
		ret = dataPageBuffer[i];
		dataPageBuffer[i] = null;
		//existingDataPagesUsed++;
		return ret;
	    }
	}
	//dataPagesAllocd++;
	return new TuplePage();
    }

    private TuplePage getCtrlPage(int ctrlMsgId) {
	if(extraCtrlPage != null) {
	    TuplePage ret = extraCtrlPage;
	    extraCtrlPage = null;
	    ret.setFlag(ctrlMsgId);
	    //existingCtrlPagesUsed++;
	    return ret;
	} else {
	    //ctrlPagesAllocd++;
	    return TuplePage.createControlPage(ctrlMsgId);
	}
    }

    private void returnCtrlPage(TuplePage returnPage) {
	if(extraCtrlPage == null)
	    System.out.println("KT dropping extra control page");
	returnPage.setFlag(CtrlFlags.NULLFLAG);
    }

    /**
     * Return a string representation of this stream
     *
     * @return the string representation of this stream
     */
    public synchronized String toString()
    {
	String retStr = new String ("\nUp Stream Tuple Queue\n");
	retStr += upStreamQueue.toString();
	retStr += "\nDown Stream Control Queue\n";
	retStr += downStreamQueue.toString();
	retStr += "\n eos: " + eos + " shutdown: " + shutdown + "\n";
	return retStr;
    }
}

