
/**********************************************************************
  $Id: Stream.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>Stream</code> provides stream functionality that is used
 * for communicating between operators. It handles both data and control
 * elements.
 *
 * @version 1.0
 *
 *
 * @see PhysicalOperator
 */

import java.util.LinkedList;


public class Stream {

    /////////////////////////////////////////////////////////////////
    // Data Members of the Stream Class
    /////////////////////////////////////////////////////////////////

    // Buffer for propagating tuple elements up stream
    //
    private Queue upStreamTupleBuffer;
    
    // Buffer for propagating control elements up stream
    //
    private LinkedList upStreamControlBuffer;

    // Buffer for propagating control information down stream
    //
    private LinkedList downStreamControlBuffer;

    // Information about whether the stream is closed - this is inherited by NetworkStream
    //
    protected boolean isClosed;
    

    //////////////////////////////////////////////////////////////////
    // Methods of the Stream Class
    //////////////////////////////////////////////////////////////////
    
    /**
     * Default Constructor without parameters to allow subclassing
     */
    public Stream()
	{
	    // Create up stream tuple buffer
	    //
	    upStreamTupleBuffer = new Queue(20);
	    
	    // Create up stream control buffer
	    //
	    upStreamControlBuffer = new LinkedList();
	    
	    // Create down stream control buffer
	    //
	    downStreamControlBuffer = new LinkedList();
	    
	    // The stream is open initially
	    //
	    isClosed = false;
	}  

    /**
     * Constructor that initializes a stream with the up stream
     * buffer size
     *   
     * @param upStreamBufferSize The size of upward tuple channel
     */

    public Stream(int upStreamBufferSize) {

	// Create up stream tuple buffer
	//
	upStreamTupleBuffer = new Queue(upStreamBufferSize);

	// Create up stream control buffer
	//
	upStreamControlBuffer = new LinkedList();

	// Create down stream control buffer
	//
	downStreamControlBuffer = new LinkedList();

	// The stream is open initially
	//
	isClosed = false;
    }    
    
    

    /** This function tells whether the stream is empty or not.
	@return true if the stream is empty, false otherwise
    */
    
    public boolean isEmpty() {
	return (upStreamControlBuffer.isEmpty() &&
	       upStreamTupleBuffer.isEmpty());
    }
    
    /**
     * This function returns the first stream element in the up stream channel
     * (both tuple and control elements). Elements in the control channel are
     * returned if any are present; only if no elements are present in the
     * control channel are tuple elements returned. If the stream is closed and
     * all the data items have been returned in previous "gets", then the function
     * returns an end of stream element. The function blocks until a stream
     * element is available in the upward channel.
     *
     * @return An element in the up stream. If the up stream is
     *         empty and the stream is closed, returns end of stream
     *         element.
     *
     * @exception java.lang.InterruptedException The thread is interupped in the
     *                                 middle of execution
     */

    public synchronized StreamElement getUpStreamElement () 
	throws java.lang.InterruptedException {

	// Wait until at least one of the up stream buffers is not empty so
	// that an element can be got from the buffers
	//
	while (upStreamControlBuffer.isEmpty() &&
	       upStreamTupleBuffer.isEmpty()) {

	    wait( );
	}

	// Get a control element in preference to a tuple element, if possible.
	// Return the result
	//
	return internalGet();
    }
    

    /**
     * This function returns the first stream element in the up stream channel
     * (both tuple and control elements). Elements in the control channel are
     * returned if any are present; only if no elements are present in the
     * control channel are tuple elements returned. If the stream is closed and
     * all the data items have been returned in previous "gets", then the function
     * returns an end of stream element. If no element is put in any of the up
     * streams for a time interval greater than the time out, the function
     * returns null. The function returns null if the thread is interrupted during
     * the wait for an element, too.
     *
     * @param timeout The timeout value in miliseconds
     *
     * @return An element in an input stream, if present; null otherwise.
     */

    public synchronized StreamElement getUpStreamElement (int timeout) {

	// If both up stream buffers are empty, then wait for the timeout interval
	// for some element to come in
	//
	if (upStreamControlBuffer.isEmpty() &&
	    upStreamTupleBuffer.isEmpty()) {
	    
	    try {
		wait(timeout);
	    }
	    catch (InterruptedException e) {

		// If the thread is interrupted during the wait, set the
		// interrupted flag so that this can be handled outside
		// if necessary
		//
		Thread.currentThread().interrupt();

		// Return a null result
		//
		return null;
	    }
	}

	// Get a control element in preference to a tuple element, if possible.
	// Return the result
	//
	return internalGet();
    }
    

    /**
     * This function returns a control element put down stream, if any
     * exists. Otherwise, it returns null. This function is non-blocking.
     *
     * @return The first control element downstream; null if there is no
     *         such element.
     *
     * @exception StreamPreviouslyClosedException If the stream was closed earlier.
     */

    public synchronized StreamControlElement getDownStreamControlElement ()
	throws StreamPreviouslyClosedException {

	// Check for end of stream and raise exception if necessary
	//
	if (isClosed) {

	    throw new StreamPreviouslyClosedException();
	}
	
	// Get first element, if any, in the down stream buffer
	//
	if (downStreamControlBuffer.isEmpty()) {

	    return null;
	}
	else {
	    return ((StreamControlElement) downStreamControlBuffer.removeFirst());
	}
    }


    /**
     * This functions puts a stream tuple element in the up stream buffer. This
     * is successful only is no control information was previously put in the
     * down stream buffer. If this was the case, then the control information is
     * returned and the output stream tuple element is not put in the up stream
     * buffer. This function blocks until either the output element can be put in
     * the up stream buffer or a control element is read from the down stream
     * buffer.
     * 
     * @param outputElement The element to be put in the up stream buffer
     *
     * @return null if put is successful; a stream control element otherwise.
     *
     * @exception java.lang.InterruptedException The thread is interrupted in
     *                                           the middle of execution.
     * @exception StreamPreviouslyClosedException The stream was previously closed
     * @exception NullElementException The output element to be put is null.
     */

    public synchronized StreamControlElement putTupleElementUpStream (
					    StreamTupleElement outputElement)
	throws java.lang.InterruptedException,
	       StreamPreviouslyClosedException,
	       NullElementException {

	// Try to put the element in the up stream buffer
	//
	return internalPut (outputElement);

    }
    

    /**
     * This functions puts a stream control element in the up stream buffer. This
     * is successful only is no control information was previously put in the
     * down stream buffer. If this was the case, then the down stream control
     * information is returned and the output stream control element is not put in
     * the up stream buffer. This function blocks until either the output element
     * can be put in the up stream buffer or a control element is read from the
     * down stream buffer.
     * 
     * @param outputElement The element to be put in the up stream buffer
     *
     * @return null if put is successful; a stream control element otherwise.
     *
     * @exception java.lang.InterruptedException The thread is interrupted in
     *                                           the middle of execution.
     * @exception StreamPreviouslyClosedException The stream was previously closed
     * @exception NullElementException The output element to be put is null.
     */

    public synchronized StreamControlElement putControlElementUpStream (
					    StreamControlElement outputElement)
	throws java.lang.InterruptedException,
	       StreamPreviouslyClosedException,
	       NullElementException {

	// Try to put the element in the up stream buffer
	//
	return internalPut (outputElement);
    }


    /**
     * This function puts a control element up stream that is delivered
     * out of order with higher priority. This function is non-blocking.
     *
     * @param controlElement the control element to be sent up stream
     *
     * @exception StreamPreviouslyClosedException the stream was previously closed.
     * @exception NullElementException The output element to be put is null.
     */

    public synchronized void priorityPutControlElementUpStream(
				       StreamControlElement controlElement)
	throws StreamPreviouslyClosedException,
	       NullElementException {

	// If the controlElement is null, raise a NullElementException
	//
	if (controlElement == null) {
	    
	    throw new NullElementException();
	}

	// Check for end of stream and raise exception if necessary
	//
	if (isClosed) {

	    throw new StreamPreviouslyClosedException();
	}

	// Add the control element to the end of the up stream control buffer
	//
	upStreamControlBuffer.add(controlElement);

	// Notify waiter if any
	//
	notify();
    }

   
    /**
     * This function puts a control element down stream. This function is
     * non-blocking.
     *
     * @param controlElement the control element to be sent
     *
     * @exception StreamPreviouslyClosedException The stream has been previously
     *                                            closed
     * @exception NullElementException the control element to be sent is null
     */

    public synchronized void putControlElementDownStream (
					  StreamControlElement controlElement)
	throws StreamPreviouslyClosedException,
	       NullElementException {

	// If the control element is null, throw an exception
	//
	if (controlElement == null) {
	    
	    throw new NullElementException();
	}

	// Check for end of stream and raise exception if necessary
	//
	if (isClosed) {

	    throw new StreamPreviouslyClosedException();
	}

	// Add the control element to the end of the down stream control buffer
	//
	downStreamControlBuffer.add(controlElement);

	// Notify waiter if any
	//
	notify();
    }


    /**
     * This function closes a stream so that no further upward or downward
     * communication (other than get) is possible. This function is non-
     * blocking.
     *
     * @exception StreamPreviouslyClosedException the stream was previously closed.
     */

    public synchronized void close () throws StreamPreviouslyClosedException {

	// If the stream was previously closed, throw an exception
	//
	if (isClosed) {

	    throw new StreamPreviouslyClosedException();
	}

	// Add an end of stream element to the up stream tuple buffer
	//
	upStreamTupleBuffer.guaranteedPut(new StreamEosElement());

	// Set a flag that the stream is closed
	//
	isClosed = true;

	// Notify waiter if any
	//
	notify();
    }


    /**
     * Return a string representation of this stream
     *
     * @return the string representation of this stream
     */
    public synchronized String toString()
    {
	String retStr = new String ("\nUp Stream Tuple Queue\n");
	retStr += upStreamTupleBuffer.toString();
	retStr += "\nUp Stream Control Queue\n";
	retStr += upStreamControlBuffer.toString();
	retStr += "\nDown Stream Control Queue\n";
	retStr += downStreamControlBuffer.toString();
	return retStr;
    }


    /**
     * This function gets a stream element from the up stream. Control
     * elements are got in preference to tuple element. In case the stream
     * is closed and all control and tuple elements in the up stream have
     * been read, then an end of stream element is returned.
     *
     * @return an element from the up stream buffer; if none, an end of
     *         stream element
     */

    private synchronized StreamElement internalGet () {

	// If the control buffer is not empty, then return the control element
	//
	if (!upStreamControlBuffer.isEmpty()) {

	    // Yes - the control buffer has some element - return that
	    //
	    return ((StreamElement) upStreamControlBuffer.removeFirst());
	}
	else {

	    // The tuple buffer has some element - get the first element
	    //
	    StreamElement firstElement = 
		               (StreamElement) upStreamTupleBuffer.get();

	    // If the element is an end of stream element, this signifies the end of
	    // stream. So, add it to up stream buffer again for correct operation of
	    // future gets. If it is not the end of stream, then notify potential
	    // thread waiting to put in the up stream buffer.
	    //
	    if (firstElement instanceof StreamEosElement) {
		upStreamControlBuffer.add(firstElement);
	    }
	    else {
		notify();
	    }

	    // Return the element read
	    //
	    return firstElement;
	}

	// End of function
    }


    /**
     * This functions puts a stream element in the up stream buffer. This
     * is successful only is no control information was previously put in the
     * down stream buffer. If this was the case, then the control information is
     * returned and the output stream element is not put in the up stream
     * buffer. This function blocks until either the output element can be put in
     * the up stream buffer or a control element is read from the down stream
     * buffer.
     * 
     * @param outputElement The element to be put in the up stream buffer
     *
     * @return null if put is successful; a stream control element otherwise.
     *
     * @exception java.lang.InterruptedException The thread is interrupted in
     *                                           the middle of execution.
     * @exception StreamPreviouslyClosedException The stream was previously closed
     * @exception NullElementException The output element to be put is null.
     */

    private synchronized StreamControlElement internalPut(
						 StreamElement outputElement)
	throws java.lang.InterruptedException,
	       StreamPreviouslyClosedException,
	       NullElementException {

	// Check to see whether the outputElement is null - if so, raise an
	// exception.
	//
	if (outputElement == null) {

	    throw new NullElementException();
	}

	// Check if stream was previously closed - if so, throw an exception
	//
	if (isClosed) {

	    throw new StreamPreviouslyClosedException();
	}

	// Wait until either the up stream tuple buffer is not full (so that the
	// outputElement can be put) or the down stream control buffer is not empty
	// (so that a control element can be returned).
	//
	while (upStreamTupleBuffer.isFull() &&
	       downStreamControlBuffer.isEmpty()) {

		wait();
	}

	// If there is a control element in the down stream buffer, then return
	// that
	//
	if (!downStreamControlBuffer.isEmpty()) {
		
	    // Return control element in down stream buffer
	    //
	    return ((StreamControlElement) downStreamControlBuffer.removeFirst());
	}	
	else {

	    // This means that there is some place in the up stream buffer. So
	    // put the output element
	    //
	    upStreamTupleBuffer.put(outputElement);

	    // Notify potential waiting thread
	    //
	    notify();

	    // Return null to indicate success of putting outputElement
	    //
	    return null;
	}	
    }

    // End of class
}
