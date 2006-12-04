
/**********************************************************************
  $Id: SinkTupleStream.java,v 1.13 2006/12/04 21:50:22 tufte Exp $


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
 * TupleStream provides a tuple-oriented stream interface for operators
 * to use. Underneath, the tuples are batched into pages and the
 * pages are passed to the next operator using a page stream. This
 * gives operators a tuple-oriented view, like they want yet avoids
 * element-by-element synchronization. This provides a sink stream
 * to be written to - a similar class provides a source stream.
 *
 * @version 1.0
 *
 */

import org.w3c.dom.Node;

public final class SinkTupleStream {

    private PageStream pageStream;
    private TuplePage buffer;

    private int status;

    // if sendImmediate is true, tuples are not buffered,
    // page is sent immediately
    private boolean sendImmediate = false;

    //possible statuses for sink streams
    public static final int Open = 0;
    public static final int Closed = 1; // closed indicates EOS

    // should we propagate get_partial requests to the operator or
    // just reflect them upwards - does not work to handle this in
    // Physical Operator - ask me and I'll explain KT
    boolean reflectPartial;

    /**
     * Constructor
     */
    public SinkTupleStream(PageStream stream) {
	this(stream, false); // default is not to reflect partial
    }  


    /**
     * Constructor
     */
    public SinkTupleStream(PageStream stream, boolean reflectPartial) {
	this.pageStream = stream;
	this.buffer = new TuplePage();
	this.reflectPartial = reflectPartial;
	status = Open;
    }  
    

    public void setSendImmediate() {
	sendImmediate = true;
    }

    /**
     * This function closes a stream so that no further upward or downward
     * communication (other than get) is possible. This function is non-
     * blocking.
     *
     */
    public void endOfStream() 
	throws java.lang.InterruptedException, ShutdownException{
	// pageStream sends an EOS up stream and sets an isClosed flag
	status = Closed;
        int ctrlFlag = putCtrlMsg(CtrlFlags.EOS, "End of Stream");
	if(ctrlFlag == CtrlFlags.GET_PARTIAL ||
	   ctrlFlag == CtrlFlags.REQUEST_BUF_FLUSH) {
	    // ignore since we just sent eos
	} else {
	    assert ctrlFlag == CtrlFlags.NULLFLAG :
	       "KT Unexpected ctrl flag " + CtrlFlags.name[ctrlFlag];
	}
	pageStream.endOfStream();
    }

    /**
     * This function returns a control element put down stream, if any
     * exists. Otherwise, it returns null. This function is non-blocking.
     * This function allows physOperator to check for control messages
     * from its sinks after reading tuples from its sources
     *
     * @return The control flag of first control element downstream; 
     *         NULLFLAG if there is no such element
     */
    public int getCtrlMsg() throws ShutdownException, 
    java.lang.InterruptedException {
	// dont check eos or shutdown,  pageStream will do it
	int ctrlFlag =  pageStream.getCtrlMsgFromSink();

	// we have to handle getpartials in case the operator or
	// stream below us can't handle them (or isn't supposed
	// to get getPartials for optimization reasons)
	// Also, we handle REQ_BUF_FLUSH
	return processCtrlFlag(ctrlFlag);
    }

    /**
     * Puts a stream tuple element to the sink (in the up stream buffer.) This
     * is successful only is no control information was previously received
     * from the sink (in the down stream buffer). If there is control 
     * information, an appropriate control flag is returned
     * This function blocks until either the output element can be put in
     * the up stream buffer or a control element is read from the down stream
     * buffer.
     * 
     * @param tuple The tuple to be passed to the sink (upstream buffer)
     *
     * @return CtrlFlags.NULLFLAG if successful, control flag otherwise
     *
     * @exception java.lang.InterruptedException The thread is interrupted in
     *                                           the middle of execution.
     */

    public int putTuple(Tuple tuple)
	throws java.lang.InterruptedException, ShutdownException {
	// try to put the tuple in the buffer, if the buffer is full
	// flush it - leave an empty buffer for next call
	
	// check eos and shutdown here - catch these more quickly this way
	assert status != Closed : "KT writing after end of stream";
	if(pageStream.shutdownReceived()) {
	    throw new ShutdownException(pageStream.getShutdownMsg());
	}
	   
	buffer.put(tuple);
	if(buffer.isFull() || sendImmediate) {
	    return flushBuffer(); // flushBuffer returns GET_PARTIAL or NULLFLAG
	} else {
	    return CtrlFlags.NULLFLAG; // success
	}
    }	

    public void putTupleNoCtrlMsg(Tuple tuple)
	throws java.lang.InterruptedException, ShutdownException {
	// try to put the tuple in the buffer, if the buffer is full
	// flush it - leave an empty buffer for next call
	
	int ctrlFlag = putTuple(tuple);
	assert ctrlFlag == CtrlFlags.NULLFLAG : 
	    "KT unexpected control message " + CtrlFlags.name[ctrlFlag];
	return;
    }	


    /**
     * put a dom node (typically a document) into the stream -
     * this is for the use of StreamScan, DTDScan, etc.
     */
    public void put(Node node) 
        throws java.lang.InterruptedException, ShutdownException {

	Tuple tuple = null;

	//Let's see if this is a punctuation or not
	Node child = node.getFirstChild();
	if (child != null) {
	    //It will be a punctuation if it prefixed with "PUNCT"
	    String name = child.getNodeName();
		String namespace = child.getNamespaceURI();
        if (namespace != null &&
				  namespace.equals("http://www.cse.ogi.edu/dot/punct")){
				tuple = new Punctuation(false, 1);
      }
	}

	if (tuple == null)
	    // create a tuple that is not a partial result
	    tuple = new Tuple(false, 1);

    // Add the object as an attribute of the tuple
    tuple.appendAttribute(node);
    int ctrlFlag = putTuple(tuple);

	// stream above an operator using this put should
	// always reflect partials
	assert ctrlFlag != CtrlFlags.GET_PARTIAL: 
	    "KT unexpected get partial request";

	// handle a flush buffer request
	ctrlFlag = processCtrlFlag(ctrlFlag);

	// stream above an operator using this put should
	// always reflect partials
	assert ctrlFlag == CtrlFlags.NULLFLAG : 
	       "KT Unexpected ctrl flag " + CtrlFlags.name[ctrlFlag];
    }

    /**
     * This functions puts a control message in the sink buffer (upstream
     * buffer). This function checks for control messages coming 
     * from the sink. If such a control message is found, or if eos
     * is encountered, an appropriate control flag is returned
     * This function blocks until either the output element
     * can be put in the up stream buffer or a control element is read from the
     * down stream buffer.
     * 
     * @param controlMsgId they type of control message to be put in stream
     *
     * @return CtrlFlags.NULLFLAG on success, control flag otherwise
     */
    public int putCtrlMsg(int controlMsgId, String ctrlMsg)
	throws java.lang.InterruptedException, ShutdownException {
	// KT control element put should cause partially full page to be 
	// sent 
	assert buffer.getFlag() == CtrlFlags.NULLFLAG :
	    "KT buffer already has a flag!";
	buffer.setFlag(controlMsgId);
	buffer.setCtrlMsg(ctrlMsg);
	int ctrlFlag = flushBuffer();
	if(ctrlFlag != CtrlFlags.NULLFLAG) {
	    // put failed! better reset the buffer
	    buffer.setFlag(CtrlFlags.NULLFLAG);
	}
	return ctrlFlag;
    }

    /**
     * flushes the buffer to the page stream and gets a new empty buffer
     * to fill up.
     * 
     * @return NULLFLAG on success, control flag if a control element
     * was encountered during buffer flushing
     * only ctrl flag returned by pageStream.putPageToSink is GET_PARTIAL
     * or NULLFLAG
     */
    private int flushBuffer() throws ShutdownException, InterruptedException {
	if(buffer.isEmpty() && buffer.getFlag() == CtrlFlags.NULLFLAG) {
	    // don't flush an empty buffer...
	    return CtrlFlags.NULLFLAG;
	} else {
	    int ctrlFlag = pageStream.putPageToSink(buffer);
	    if(ctrlFlag == CtrlFlags.NULLFLAG) {
		// success
		// get new empty buffer to write in
		buffer = pageStream.getTuplePage();
		buffer.startPutMode();
	    } else {
		ctrlFlag = processCtrlFlag(ctrlFlag);
	    }
	    assert ctrlFlag == CtrlFlags.NULLFLAG ||
		ctrlFlag == CtrlFlags.GET_PARTIAL : 
	       "KT Unexpected ctrl flag " + CtrlFlags.name[ctrlFlag];
	    return ctrlFlag;
	}
    }

    /**
     * SinkTupleStream handles some control messages - this function
     * makes sure the handling is uniform. SinkTupleStream handles
     * some GET_PARTIALS and some REQ_BUF_FLUSHES
     * 
     * @param ctrlFlag The control flag received
     *
     * @returns The ctrl flag to be returned to PhysicalOperator.
     *          Should be either NULLFLAG OR GET_PARTIAL
     */
    private int processCtrlFlag(int ctrlFlag) 
	throws ShutdownException, InterruptedException {

	switch(ctrlFlag) {
	case CtrlFlags.REQUEST_BUF_FLUSH:
	    if(PageStream.VERBOSE)
		System.out.println(pageStream.getName() + 
				   " received request for buffer flush ");
	    return flushBuffer(); // returns NULLFLAG or GET_PARTIAL
	    
	case CtrlFlags.GET_PARTIAL:
	    if(reflectPartial) {
		reflectPartial(); // void
		return CtrlFlags.NULLFLAG;
	    } else {
		return CtrlFlags.GET_PARTIAL;
	    }

	case CtrlFlags.NULLFLAG:
	    return CtrlFlags.NULLFLAG;
	    
	default:
	    assert false : "KT Unexpected control flag" 
		+ CtrlFlags.name[ctrlFlag];
	    return CtrlFlags.NULLFLAG;
	}
    }


    /**
     * put a SYNC_PARTIAL into the sink stream (upstream). This function
     * is called when a GET_PARTIAL is received downstream and the
     * operator using this SinkTupleStream can not/should not handle
     * GET_PARTIAL messages
     *
     * calls putCtrlMsg (which calls flushBuffer) - this may recurse,
     * but will always result in the buffer being sent
     */
    private void reflectPartial() 
	throws InterruptedException, ShutdownException {
	// set buffer flag to SYNCH_PARTIAL and try to flushBuffer
	// if flushBuffer fails, it will return GET_PARTIAL - unlikely
	// since I just got a GET_PARTIAL - can not just drop that GET_PARTIAL
	// because others might bounce it back 

	// if reflectPartial is true - flushBuffer and therefore 
	// putCtrlMsg will always return NULLFLAG
	int ctrlFlag = putCtrlMsg(CtrlFlags.SYNCH_PARTIAL, null);
	assert ctrlFlag == CtrlFlags.NULLFLAG : 
	       "KT Unexpected ctrl flag " + CtrlFlags.name[ctrlFlag];
    }

    /**
     * Return a string representation of this stream
     *
     * @return the string representation of this stream
     */
    public String toString()
    {
	String retStr = new String ("\nTuple Buffer \n");
	retStr += buffer.toString();
	retStr += "\nPage Stream\n";
	retStr += pageStream.toString();
	retStr += "\nStatus \n" + status;
	return retStr;
    }

    public int getStatus() {
	return status;
    }

    public boolean isClosed() {
	return status == Closed;
    }
}
