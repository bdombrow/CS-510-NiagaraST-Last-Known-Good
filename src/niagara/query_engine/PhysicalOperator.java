
/**********************************************************************
  $Id: PhysicalOperator.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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


package niagara.query_engine;

/**
 * This is the Abstract <code>PhysicalOperator</code> class from which
 * all operator are derived. It provides the basic control flow and all
 * each operator has to do is to implement stubs.
 *
 * @version 1.0
 */

import java.lang.reflect.Array;
import java.util.ArrayList;
import niagara.utils.*;
import niagara.data_manager.*;

public abstract class PhysicalOperator {
	
    ////////////////////////////////////////////////////////
    //   Data members private to the PhysicalOperator Class
    ////////////////////////////////////////////////////////

    /**
     * The list of source Streams
     */
    private Stream[] sourceStreams;

    /**
     * Information about the current status of the source streams
     */
    private int[] sourceStreamsStatus;

    /**
     * Information about which source streams an operator blocks on
     */

    private boolean[] blockingSourceStreams;

    /**
     * The list destination Streams
     */
    private Stream[] destinationStreams;

    /**
     * Information about the current status of destination streams
     */
    private int[] destinationStreamsStatus;

    /**
     * Counts of duplicated partial result request messages expected in each
     * of the destination streams
     */
    private int[] destinationStreamsPartialResultCount;

    /**
     * The required responsiveness to control messages
     */
    private int responsiveness;

    /**
     * The vector of source streams to read from
     */
    private ArrayList readSourceStreams;

    /**
     * The index (in readInputStreams) of the last source stream read
     */
    private int lastReadSourceStream;

    /**
     * The reference to the data manager
     */
    static DataManager DM; //Trigger 

    ///////////////////////////////////////////////////
    // Nested classes private to Physical Operator
    ///////////////////////////////////////////////////

    /**
     * This class defines the possible status of source streams
     */

    private static class SourceStreamStatus {

	public static final int Open = 0;
	public static final int SynchronizePartialResult = 1;
	public static final int EndOfPartialResult = 2;
	public static final int Closed = 3;
    }


    /**
     * This class defines the possible status of destination streams
     */

    private static class DestinationStreamStatus {

	public static final int Open = 0;
	public static final int Closed = 1;
    }


    /**
     * This class is used to store the result of a read operation from
     * a set of source streams. There are two components (a) the stream element
     * read and (b) the stream from which the result was read.
     */

    private class SourceStreamsObject {

	public StreamTupleElement element;
	public int streamId;
    }


    ////////////////////////////////////////////////////////////////////
    // Nested classes visible to derived classes of Physical Operator //
    ////////////////////////////////////////////////////////////////////

    /**
     * This class stores the tuples to be sent to destination streams, along
     * with the id of the destination stream
     */

    protected class ResultTuples {

	// Storage for tuples
	//
	private ArrayList tuples;
	
	// Storage for destination stream ids
	//
	private ArrayList streams;


	/**
	 * This is the constructor that initializes the storage
	 */

	public ResultTuples () {

	    this.tuples = new ArrayList();
	    this.streams = new ArrayList();
	}


	/**
	 * This function adds a tuple element along with a destination stream
	 * id
	 *
	 * @param tuple The tuple to be added
	 * @param streamId The id of the destination stream for the tuple
	 */

	public void add (StreamTupleElement tuple, int streamId) {

	    tuples.add(tuple);
	    streams.add(new Integer(streamId));
	}


	/**
	 * This function return the number of tuples in the result
	 *
	 * @return The number of tuples in this result object
	 */

	public int size () {
	    return tuples.size();
	}


	/**
	 * This function return the tuple given its index
	 *
	 * @param index The index of the tuple to be retrieved
	 *
	 * @return The desired tuple
	 */

	public StreamTupleElement getTuple (int index) {
	    return (StreamTupleElement) tuples.get(index);
	}


	/**
	 * This function return the destination stream id, given its index
	 *
	 * @param index The index of the stream id to be retrieved
	 *
	 * @return The desired stream id
	 */

	public int getStreamId (int index) {
	    return ((Integer) streams.get(index)).intValue();
	}


	/**
	 * This function clears the entries in the object
	 */

	public void clear () {
	    tuples.clear();
	    streams.clear();
	}
    }


    ///////////////////////////////////////////////////
    //   Methods of the PhysicalOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the Operator class that initializes
     * it with the appropriate logical operator, source streams,
     * destination streams, whether the operator is blocking or non-blocking
     * and the responsiveness of the operator to control information.
     *
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param blockingSourcetreams A boolean array that specifies the source
     *                             streams due to which the operator blocks.
     * @param responsiveness The responsiveness, in milliseconds, to control
     *                       messages
     */
     
    public PhysicalOperator (Stream[] sourceStreams,
			     Stream[] destinationStreams,
			     boolean[] blockingSourceStreams,
			     Integer responsiveness) {

	// Call the constructor of the super class
	//
	super();

	// Initialize the source Streams
	//
	this.sourceStreams = sourceStreams;

	// Set the status of every source stream to blocked
	//
	int numSourceStreams = Array.getLength(sourceStreams);

	sourceStreamsStatus = new int[numSourceStreams];

	for (int src = 0; src < numSourceStreams; ++src) {
	    sourceStreamsStatus[src] = SourceStreamStatus.Open;
	}

	// Initialize the destination Streams
	//
	this.destinationStreams = destinationStreams;

	// Set the status of every destination stream to open
	//
	int numDestStreams = Array.getLength(destinationStreams);

	destinationStreamsStatus = new int[numDestStreams];

	for (int dest = 0; dest < numDestStreams; ++dest) {
	    destinationStreamsStatus[dest] = DestinationStreamStatus.Open;
	}

	// Initialize which source streams the operator blocks on
	//
	this.blockingSourceStreams = blockingSourceStreams;

	// Get the number of duplicate partial request messages in each
	// destination stream to 0
	//
	destinationStreamsPartialResultCount = new int[numDestStreams];

	for (int dest = 0; dest < numDestStreams; ++dest) {
	    destinationStreamsPartialResultCount[dest] = 0;
	}

	// Initialize the responsiveness
	//
	this.responsiveness = responsiveness.intValue();

	// Initially, have to read from all input streams
	//
	readSourceStreams = new ArrayList(numSourceStreams);

	for (int src = 0; src < numSourceStreams; ++src) {
	    readSourceStreams.add(new Integer(src));
	}

	// Start reading from the first input stream
	//
	//lastReadSourceStream = numSourceStreams - 1;
	lastReadSourceStream = 0;
    }
		     

    /**
     * This function sets up the flow of control for the operator by
     * reading from input streams and writing to destination streams. All
     * control messages are handled here.
     */

    public final void execute () {

	// Flag that indicates whether the operator is to proceed or quit
	//
	boolean proceed = false;

	// Storage for tuples to be put in destination streams
	//
	ResultTuples resultTuples = new ResultTuples();

	// Set up an object for reading from source streams
	//
	SourceStreamsObject sourceObject = new SourceStreamsObject();

	try {
	    // First initialize any necessary data structures etc. for the
	    // operator
	    //
	    proceed = this.initialize();

	    // If not proceeding, shut down operator
	    //
	    if (!proceed) {

		shutDownOperator();
	    }

	    // Loop by reading inputs and processing them until there is at
	    // least one open input stream
	    //
	    while (!Thread.currentThread().isInterrupted() &&
		   proceed && 
		   numUnClosedSourceStreams() > 0) {
		
		// Read the object from any of the valid input streams,
		// timing out if nothing available in any input stream
		//
		proceed = getFromSourceStreams(sourceObject);

		if (proceed && sourceObject.element != null) {
		   
		    // There was some tuple element read, so process it and get
		    // the output elements to be sent
		    //
		    if (isBlocking()) {

			// Process tuple element in a blocking fashion
			//
			proceed = this.blockingProcessSourceTupleElement(
				       (StreamTupleElement) sourceObject.element,
				       sourceObject.streamId);

			// Shut down operator if necessary
			//
			if (!proceed) {

			    shutDownOperator();
			}
		    }
		    else {

			// Process tuple element in a non-blocking fashion
			//
			proceed = this.nonblockingProcessSourceTupleElement(
				        (StreamTupleElement) sourceObject.element,
					sourceObject.streamId,
					resultTuples);

			// If the operators wants to shut down, do so
			//
			if (!proceed) {

			    shutDownOperator();
			}
			else if (resultTuples.size() > 0) {

			    // Write elements to destination streams and handle
			    // destination control elements if any
			    //
			    proceed = putToDestinationStreams(resultTuples);

			    // Clear resultTuples for next loop
			    //
			    resultTuples.clear();
			}
		    }
		}

		// Now check to see whether there are any control elements from
		// the destination streams and process them if so.
		//
		if (proceed) {
		    proceed = checkDestinationControlElements();
		}
	    }

	    // If the thread was interrupted, throw an interrupted exception
	    //
	    if (proceed && Thread.currentThread().isInterrupted()) {
		throw new java.lang.InterruptedException();
	    }
	}
	catch (java.lang.InterruptedException e) {

	    // The thread has been interrupted - so shut down operator
	    //
	    shutDownOperator();
	}
	catch (NullElementException e) {

	    // This must be due to a programming error
	    //
	    System.err.println("Programming Error in Operator");

	    shutDownOperator();
	}
	catch (StreamPreviouslyClosedException e) {

	    // This must be due to a programming error
	    //
	    System.err.println("Programming Error in Operator");

	    shutDownOperator();
	}

	// Close all open destination streams
	//
	try {
	    closeDestinationStreams();
	}
	catch (StreamPreviouslyClosedException e) {
	    System.err.println("Error in Operator: Closing a Closed Stream");
	}

	// Do cleaning up for operator, if any
	//
	this.cleanUp();
    }


    /////////////////////////////////////////////////////////////////////
    // The following functions are utility functions used for setting  //
    // up the flow of control of an operator.                          //
    /////////////////////////////////////////////////////////////////////

    /**
     * This function returns the number of source streams that are not closed
     *
     * @return The number of un-closed source streams
     */

    private int numUnClosedSourceStreams () {

	// Intialize the result
	//
	int result = 0;

	// Loop over all source streams counting the number of unclosed ones
	//
	int numSourceStreams = Array.getLength(sourceStreams);

	for (int src = 0; src < numSourceStreams; ++src) {
	    if (getSourceStreamStatus(src) != SourceStreamStatus.Closed) {
		++result;
	    }
	}

	// Return the result
	//
	return result;
    }


    /**
     * This functions writes the current output to the destination streams
     *
     * @param partial If the output is to be tagged as partial
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted
     *            during execution
     * @exception NullElementException An attempt is made to put a null
     *            element to output
     * @exception StreamPreviouslyClosedException An attempt is made to
     *            write to a previously closed stream
     */

    private boolean putCurrentOutput (boolean partial)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	ResultTuples resultTuples = new ResultTuples();

	// Get the current output of the operator
	//
	boolean proceed = this.getCurrentOutput(resultTuples, partial);

	// If the operator is not to proceed, then shut down operator
	//
	if (!proceed) {

	    shutDownOperator();
	}
	else {

	    // Write elements to destination streams and handle
	    // destination control elements if any
	    //
	    proceed = putToDestinationStreams(resultTuples);
	}

	// Return information about whether operator is to proceed or not
	//
	return proceed;
    }


    /**
     * This function returns true if the operator is blocking and false
     * otherwise
     *
     * @return True if the operator is blocking and false otherwise
     */

    private boolean isBlocking () {

	// If any blocking source stream is not closed, then return false
	// else return true
	//
	int numSourceStreams = Array.getLength(sourceStreams);

	for (int src = 0; src < numSourceStreams; ++src) {

	    if (blockingSourceStreams[src] &&
		sourceStreamsStatus[src] != SourceStreamStatus.Closed) {

		// Operator is blocking
		//
		return true;
	    }
	}

	// Operator is non-blocking
	//
	return false;
    }


    /**
     * This function propagates shut down messages to all source
     * and destination streams
     */

    protected void shutdownTrigOp() {
        // System.err.println("### --- %%%% shutdown called");
        return;
    }

    private void shutDownOperator () {
	
	try {
            shutdownTrigOp();
	    propagateOperatorShutDownElement(new StreamControlElement(
					      StreamControlElement.ShutDown));
	}
	catch (java.lang.InterruptedException e) {
	}
	catch (NullElementException e) {
	    System.err.println("NonNull Element is Sent!");
	}
	catch (StreamPreviouslyClosedException e) {
	    System.err.println("Programming Error in Operator");
	}
    }



    ////////////////////////////////////////////////////////////////////////
    // The following functions are used to provide easy access to streams //
    // within this class.                                                 //
    ////////////////////////////////////////////////////////////////////////

    /**
     * This function sets the status of a source stream
     *
     * @param streamId The id of the stream whose status is to be set
     * @param status The status to which the stream is to be set
     */

    private void setSourceStreamStatus (int streamId, int status) {

	sourceStreamsStatus[streamId] = status;
    }


    /**
     * This function sets the status of a destination stream
     *
     * @param streamId The id of the stream whose status is to be set
     * @param status The status to which the stream is to be set
     */

    private void setDestinationStreamStatus (int streamId, int status) {

	destinationStreamsStatus[streamId] = status;
    }


    /**
     * This function reads from a set of source streams specified by
     * the variable readSourceStreams and modifies sourceObject that
     * contains the source element read and the stream from which it was
     * read. If no source element was read, the the element field in
     * sourceObject is set to null. This operation splits the
     * responsiveness time of the operator among all the source streams it
     * can read from. If this responsiveness time is exceeded, it returns null
     * in the element field of sourceObject.
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thrown if the thread is
     *            interrupted during execution
     * @exception NullElementException An attempt is made to put a null
     *            element to a stream
     * @exception StreamPreviouslyClosedException An attempt is made to
     *            write to a previously closed stream
     */

    private boolean getFromSourceStreams (SourceStreamsObject sourceObject)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Get the number of source streams to choose from
	//
	int numReadSourceStreams = readSourceStreams.size();

	// Calculate the time out for each stream
	//
	int timeout = responsiveness/numReadSourceStreams;

	// Make sure the last read source stream is a valid index
	//
	if (lastReadSourceStream >= numReadSourceStreams) {
	    //lastReadSourceStream = numReadSourceStreams - 1;
		lastReadSourceStream = 0;
	}

	// Start from next source stream
	//
	lastReadSourceStream = 
	    (lastReadSourceStream + 1)%numReadSourceStreams;

	// Store this stream index
	//
	int startReadSourceStream = lastReadSourceStream;

	// Loop over all source streams, until a full round is completed
	//
	do {

	    // Get the next stream id to read from
	    //
	    int streamId = 
		((Integer)
		 readSourceStreams.get(lastReadSourceStream)).intValue();

	    // Wait for input from a source stream until the time out
	    //
	    StreamElement sourceElement = 
		sourceStreams[streamId].getUpStreamElement(timeout);

	    // If there was an element read, return it
	    //
	    if (sourceElement != null) {

			if (sourceElement instanceof StreamControlElement) {
				
				// This is a control element, so process it
				//
				boolean proceed = processSourceControlElement(
					(StreamControlElement) sourceElement,
					streamId);
				
				// No tuple element to be returned
				//
				sourceObject.element = null;
				
				// Return whether operator is to proceed
				//
				return proceed;
			}
			else if (sourceElement instanceof StreamEosElement) {
				
				// This is the end of stream, so mark the stream as closed
				//
				setSourceStreamStatus(streamId,
									  SourceStreamStatus.Closed);
				
				// Remove the stream id from the list
				//
				readSourceStreams.remove(lastReadSourceStream);
				
				// No element to return
				//
				sourceObject.element = null;
				
				boolean proceed = true;
				
				// If this causes the operator to become non-blocking, then
				// put out the current output and clear the current output
				//
				if (blockingSourceStreams[streamId] && !isBlocking()) {
					
					// Put out the current output (which is not partial anymore)
					//
					proceed = putCurrentOutput(false);
					
					if (proceed) {
						
						// Clear the current output
						//
						proceed = this.clearCurrentOutput();
						
						// Shut down operator if necessary
						//
						if (!proceed) {
							
							shutDownOperator();
						}
					}
				}
				
				// Update partial result creation if any. This is necessary because
				// partial result creation may terminate when all input streams
				// are either synchronized or closed.
				//
				if (proceed) {
					proceed = updatePartialResultCreation();
				}
				
				// Return whether the operator is to proceed
				//
				shutdownTrigOp();
				return proceed;
			}
			else {
				
				// This has to be a tuple element - set the appropriate
				// values for the result
				//
				sourceObject.element = (StreamTupleElement) sourceElement;
				sourceObject.streamId = streamId;
				
				// The operator can continue
				//
				return true;
			}
	    } else {
			// Try the next source stream
			//
			lastReadSourceStream = (lastReadSourceStream + 1)%numReadSourceStreams;
	    }
	} while (startReadSourceStream != lastReadSourceStream);

	// No luck with any source stream
	//
	sourceObject.element = null;

	// Operator can continue
	//
	return true;
    }


    /**
     * This function takes in an ArrayList of DestinationStreamsObjects
     * and puts the stream tuple element in each object in the array list
     * to the appropriate destination stream
     *
     * @param result The result tuples to be sent to destination streams  
     *
     * @return True if operator is to continue; false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted during
     *            execution
     * @exception NullElementException One of the destination objects has a
     *            null element
     * @exception StreamPreviouslyClosedException An attempt is made to send
     *            an element to previously closed stream
     */

    private boolean putToDestinationStreams (ResultTuples resultTuples)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Loop over all the result tuples and send them to appropriate
	// destination streams
	//
	int numResultTuples = resultTuples.size();

	boolean proceed = true;

	for (int dest = 0; proceed && dest < numResultTuples; ++dest) {

	    // Get the appropriate tuple and stream id
	    //
	    StreamTupleElement resultTuple = resultTuples.getTuple(dest);
	    int resultStreamId = resultTuples.getStreamId(dest);

	    // If the required stream is already closed, continue with rest
	    // of output
	    //
	    if (getDestinationStreamStatus(resultStreamId) ==
		DestinationStreamStatus.Closed) {
		continue;
	    }

	    // Set up looping variable
	    //
	    boolean sent = false;

	    // Loop until either the required element is sent or the operator
	    // is to quit
	    //
	    while (!sent && proceed) {

		// Try to send to the required destination stream
			//        if(destinationStreams[resultStreamId]==null) continue;
		StreamControlElement controlElement = 
		    destinationStreams[resultStreamId].putTupleElementUpStream(
							             resultTuple);

		// Check if it was successfull
		//
		if (controlElement == null) {
		    sent = true;
		}
		else {
		    // Propagate the control element read
		    //
		    proceed = processDestinationControlElement(controlElement,
							    resultStreamId);
		}
	    }
	}

	// The operator is to continue if proceed is true
	//
	return proceed;
    }


    /**
     * This function checks the destionation streams for any control messages
     * and processes them if necessary
     *
     * @return True if the operator is to continue; false otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception StreamPreviouslyClosedException If a control element
     *            is checked for in a previously closed stream
     * @exception NullElementException If a null element is attempted to
     *            be put in a stream
     */

    private boolean checkDestinationControlElements () 
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Loop over all destination streams, checking for control elements
	//
	int numDestinationStreams = getNumDestinationStreams();

	for (int dest = 0; dest < numDestinationStreams; ++dest) {

	    // Make sure the stream is not closed before checking is done
	    //
        //    if(destinationStreams[dest]==null) continue;
	    if (getDestinationStreamStatus(dest) !=
		DestinationStreamStatus.Closed) {

		StreamControlElement controlElement = 
		    destinationStreams[dest].getDownStreamControlElement();

		// If a control element is read, process it
		//
		if (controlElement != null) {

		    boolean proceed = processDestinationControlElement(
							       controlElement,
							       dest);

		    // If the operator is to be quit, return
		    //
		    if (!proceed) return false;
		}
	    }
	}

	// The operator can continue
	//
	return true;
    }


    /**
     * This function closes all destination streams that have not previously
     * been closed.
     *
     * @exception StreamPreviouslyClosedException An attempt is made to close
     *            a previously closed stream
     */

    private void closeDestinationStreams () 
	throws StreamPreviouslyClosedException {

	boolean error = false;

	// Loop over all unclosed destination streams and close
	// them
	//
	int numDestinationStreams = getNumDestinationStreams();

	for (int dest = 0; dest < numDestinationStreams; ++dest) {
            
		//if(destinationStreams[dest]==null) continue;
	    if (getDestinationStreamStatus(dest) !=
		DestinationStreamStatus.Closed) {
			//System.err.println("Trying to close upStream");
		try {
		    destinationStreams[dest].close();
		}
		catch (StreamPreviouslyClosedException e) {
		    error = true;
		}
	    }
	}

	// If there is an error, throw an exception
	//
	if (error) {
	    throw new StreamPreviouslyClosedException();
	}
    }


    /////////////////////////////////////////////////////////////////////
    // The following functions process the control elements got from   //
    // the source streams.                                             //
    /////////////////////////////////////////////////////////////////////

    /**
     * This function processes a control element from a source stream.
     *
     * @param controlElement The control element read
     * @param streamId The id of the source stream from which the control
     *                 element was read
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted
     *            during execution
     * @exception NullElementException An attempt is made to put a
     *            null element to a stream
     * @exception StreamPreviouslyClosedException An attempt is made to
     *            write to a previously closed stream
     */

    private boolean processSourceControlElement (
				      StreamControlElement controlElement,
				      int streamId)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	switch (controlElement.type()) {

	case StreamControlElement.ShutDown:

	    // Propagate the shut down element to other streams if necessary
	    //
 	    return this.propagateSourceShutDownElement(controlElement, streamId);

	case StreamControlElement.SynchronizePartialResult:

	    // Remove the stream id from the set of source streams to read from
	    //
	    readSourceStreams.remove(readSourceStreams.indexOf(new Integer(streamId)));
	    
	    // Set the status of the source stream to synchronizing partial results
	    //
	    setSourceStreamStatus(streamId, 
				  SourceStreamStatus.SynchronizePartialResult);

	    // Need to handle the creation of partial results
	    //
	    return updatePartialResultCreation ();

	case StreamControlElement.EndPartialResult:

	    // Remove the stream id from the set of source streams to read from
	    //
	    readSourceStreams.remove(readSourceStreams.indexOf(new Integer(streamId)));

	    // Set the status of the source stream to end of partial results
	    //
	    setSourceStreamStatus(streamId,
				  SourceStreamStatus.EndOfPartialResult);

	    // Need to handle the creation of partial results
	    //
	    return updatePartialResultCreation ();

	default:

	    // Unhandled control element, just ignore
	    //
	    return true;
	}
    }


    /**
     * This function handles the creation of partial results
     *
     * @return True if the operator is to continue execution and false
     *         otherwise.
     *
     * @exception java.lang.InterruptedException Thrown if the thread is
     *            interrupted during execution
     * @exception NullElementException An attempt is made to put a null
     *            element into an output stream
     * @exception StreamPreviouslyClosedException An attempt is made to
     *            write to a previously closed stream
     */

    private boolean updatePartialResultCreation ()
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// If readSourceStreams is not empty, then synchronization has not
	// happened yet
	//
	if (!readSourceStreams.isEmpty()) {

	    // The operator can continue
	    //
	    return true;
	}
	else {

	    // Synchronization has happened, check whether partial results are
	    // required at all
	    //
	    boolean isPartialResult = false;

	    int numSourceStreams = getNumSourceStreams();

	    for (int src = 0; src < numSourceStreams && !isPartialResult; ++src) {

		int streamStatus = getSourceStreamStatus(src);

		if (streamStatus == SourceStreamStatus.SynchronizePartialResult ||
		    streamStatus == SourceStreamStatus.EndOfPartialResult) {
		    
		    isPartialResult = true;
		}
	    }

	    // If there are no partial results, then nothing to do
	    //
	    if (!isPartialResult) {

		// Operator can continue
		//
		return true;
	    }

	    // There are partial results, so write them out
	    //
	    boolean proceed = true;

	    // Write out the (partial) current output, if this is a blocking operator
	    //
	    if (isBlocking()) {
		proceed = putCurrentOutput(true);
	    }

	    if (!proceed) return false;

	    // Variable to check whether the partial output is partial or
	    // just synchronized
	    //
	    boolean isPartialOutput = isBlocking();

	    // Loop over all the source streams and change all streams
	    // with partial results to Open
	    //
	    for (int src = 0; src < numSourceStreams; ++src) {

		if (getSourceStreamStatus(src) == 
		    SourceStreamStatus.SynchronizePartialResult) {

		    // This is a non-blocking stream, so first make it
		    // open 
		    //
		    setSourceStreamStatus(src, SourceStreamStatus.Open);

		    // Now add it to the list of source streams to read from
		    //
		    readSourceStreams.add(new Integer(src));
		}
		else if (getSourceStreamStatus(src) ==
			 SourceStreamStatus.EndOfPartialResult) {

		    // This is a blocking stream, so remove traces of
		    // partial result
		    //
		    proceed = this.removeEffectsOfPartialResult (src);

		    // Shut down and return if operator is not to proceed
		    //
		    if (!proceed) {

			shutDownOperator();
			return false;
		    }

		    // Make the stream open
		    //
		    setSourceStreamStatus(src, SourceStreamStatus.Open);

		    // Add it to the list of source streams to read from
		    //
		    readSourceStreams.add(new Integer(src));

		    // The result is a partial result
		    //
		    isPartialOutput = true;
		}
	    }

	    // If the output is a partial result, then put the appropriate
	    // control element, else just put a SynchronizePartial control
	    // element
	    //
	    if (isPartialOutput) {
		proceed = putControlElementToDestinationStreams(new
		   StreamControlElement(StreamControlElement.EndPartialResult));
	    }
	    else {
		proceed = putControlElementToDestinationStreams(new
		   StreamControlElement(
				 StreamControlElement.SynchronizePartialResult));
	    }

	    return proceed;
	}
    }


    /////////////////////////////////////////////////////////////////////
    // The following functions process the control elements got from   //
    // the destination streams.                                        //
    /////////////////////////////////////////////////////////////////////

    /**
     * This function processes a control element from a destination stream.
     *
     * @param controlElement The control element read
     * @param streamId The id of the destination stream from which the control
     *                 element was read
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception NullElementException Thrown if controlElement is null
     * @exception StreamPreviouslyClosedException If an attempt is made to
     *            put a control element in a previously closed stream
     */

    private boolean processDestinationControlElement (
				      StreamControlElement controlElement,
				      int streamId)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	switch (controlElement.type()) {

	case StreamControlElement.ShutDown:

	    // Propagate the shut down element to other streams if necessary
	    //
            System.err.println("***************************");
            System.err.println("Got Shutting down " + this);
            System.err.println("***************************");
	    return propagateDestinationShutDownElement(controlElement, streamId);

	case StreamControlElement.GetPartialResult:

	    // Handle the get partial result request appropriately
	    //
	    return processDestinationGetPartialResultElement(controlElement,
							     streamId);

	default:

	    // Unhandled control element, just ignore
	    //
	    return true;
	}
    }


    /**
     * This function propagates Shut Down control messages received from
     * a destination stream.
     *
     * @param controlElement The shut down control element to be propagated
     * @param streamId The id of the destination stream from which the control
     *                 element was read.
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception NullElementException Thrown when controlElement is null
     */

    protected boolean propagateDestinationShutDownElement (
					StreamControlElement controlElement,
					int streamId) 
	throws NullElementException {

	// Mark the appropriate destination stream as closed
	//
	setDestinationStreamStatus(streamId, DestinationStreamStatus.Closed);

	// If at least one destination stream is unclosed, then continue
	//
	int numDestinationStreams = getNumDestinationStreams();

	for (int dest = 0; dest < numDestinationStreams; ++dest) {

	    if (getDestinationStreamStatus(dest) != 
		DestinationStreamStatus.Closed) {
		
		return true;
	    }
	}

	// No destination streams are unclosed - propagate control message to all
	// unclosed source streams
	//
	boolean proceed = putControlElementToSourceStreams(controlElement);

	// Quit the operator
	//
	return false;
    }


    /**
     * This function processes a GetPartialResult control element received
     * from a destination stream. If the control element is not a duplicate
     * previously received from some other destination stream, then it is
     * propagated using the propagation function 
     * <code>propagateDestinationGetPartialResultElement</code>. Else it
     * is suppressed.
     *
     * @param controlElement The control element read from a destination
     *                       stream
     * @param streamId The id of the destination stream from which the control
     *                 element was read
     *
     * @return True if the operator is to proceed and false otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception NullElementException If controlElement is null
     * @exception StreamPreviouslyClosedException If an attempt is made to put
     *            a control element into a previously closed stream
     */

    private boolean processDestinationGetPartialResultElement (
				      StreamControlElement controlElement,
				      int streamId)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	if (destinationStreamsPartialResultCount[streamId] == 0) {

	    // This is not a duplicate control element - so update
	    // counts on all other destination streams
	    //
	    int numDestStreams = getNumDestinationStreams();

	    for (int dest = 0; dest < numDestStreams; ++dest) {

		if (dest != streamId) {
		    ++destinationStreamsPartialResultCount[dest];
		}
	    }

	    // Propagate the control element
	    //
	    return this.propagateDestinationGetPartialResultElement(controlElement,
								    streamId);
	}
	else {

	    // This is a duplicate control element
	    //
	    --destinationStreamsPartialResultCount[streamId];

	    // The operator can continue
	    //
	    return true;
	}
    }

						       
    /////////////////////////////////////////////////////////////////////
    // The following functions process the control elements got from   //
    // the operator.                                                   //
    /////////////////////////////////////////////////////////////////////

    /**
     * This function propagates a shut down control element from the
     * operator. All unclosed source and destination streams are shut down.
     *
     * @param controlElement The control element to be propagated.
     *
     * @return False (as operator is to be shut down)
     *
     * @exception java.lang.InterruptedException Thrown if the thread is
     *            interrupted in the middle of execution
     * @exception NullElementException Thrown if controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to put a control
     *            element in a previously closed stream
     */

    private boolean propagateOperatorShutDownElement (
				   StreamControlElement controlElement)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Propagate the shut down control element to all unclosed source
	// streams
	//
	boolean proceed = putControlElementToSourceStreams(controlElement);

	// Propagate the shud down control element to all unclosed destination
	// streams
	//
	proceed = priorityPutControlElementToDestinationStreams(
							    controlElement);

	// Operator is to be shut down
	//
	return false;
    }


    //////////////////////////////////////////////////////////////////
    // Methods visible to derived classes of PhysicalOperator (also //
    // used by member functions of PhysicalOperator) used to        //
    // manipulate input and output streams                          //
    //////////////////////////////////////////////////////////////////

    /**
     * This function returns the number of source streams
     *
     * @return The number of source streams
     */

    protected final int getNumSourceStreams () {

	return Array.getLength(sourceStreams);
    }


    /**
     * This function returns the number of destination streams
     *
     * @return The number of destination streams
     */

    protected final int getNumDestinationStreams () {

	return Array.getLength(destinationStreams);
    }

    /**
     * This function returns the ith destination streams
     *
     * @return The ith destination streams
     */

    protected final Stream getDestinationStream (int i) {
	return destinationStreams[i];
    }

    /**
     * This function set the ith destination streams
     *
     * @param index and the outputStream
     * 
     */

    protected final void setDestinationStream (int i, Stream outputStream) {
	destinationStreams[i]=outputStream;
    }

    /**
     * This function returns the status of a source stream given its id
     *
     * @param streamId The id of the source streams whose status is to be
     *                 determined
     *
     * @return The status of the source stream with id streamId
     */

    protected final int getSourceStreamStatus (int streamId) {

	return sourceStreamsStatus[streamId];
    }


    /**
     * This function returns the status of a destination stream given its id
     *
     * @param streamId The id of the destination stream whose status is to be
     *                 determined
     *
     * @return The status of the destination stream with id streamId
     */

    protected final int getDestinationStreamStatus (int streamId) {

	return destinationStreamsStatus[streamId];
    }


    /**
     * This function puts a control element to a source stream
     *
     * @param controlElement The control element to be put
     * @param streamId The source stream to which the control element
     *                 is to be put
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception NullElementException controlElement is null
     */

    protected final boolean putControlElementToSourceStream (
					  StreamControlElement controlElement,
					  int streamId) 
	throws NullElementException {

	// Dont worry if the stream has been closed already
	//
	try {
	    sourceStreams[streamId].putControlElementDownStream(controlElement);
	}
	catch (StreamPreviouslyClosedException e) {
	}

	// Continue with the operator
	//
	return true;
    }


    /**
     * This function puts a control element to all source streams
     *
     * @param controlElement The control element to be sent
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception NullElementException controlElement is null
     */

    protected final boolean putControlElementToSourceStreams(
				       StreamControlElement controlElement)
	throws NullElementException {

	// Loop over all source streams and put the control
	// element in all open ones
	//
	int numSourceStreams = getNumSourceStreams();

	for (int src = 0; src < numSourceStreams; ++src) {

	    if (getSourceStreamStatus(src) != SourceStreamStatus.Closed) {

		boolean proceed = 
		    putControlElementToSourceStream(controlElement, src);

		if (!proceed) return false;
	    }
	}

	// Continue with the operator
	//
	return true;
    }


    /**
     * This function puts a control element to a destination stream
     *
     * @param controlElement The control element to be put
     * @param streamId The destination stream to which the control element
     *                 is to be put
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted
     *            during execution
     * @exception NullElementException controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to put controlElement
     *            in a previously closed stream
     */

    protected final boolean putControlElementToDestinationStream (
					  StreamControlElement controlElement,
					  int streamId) 
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Put the control element up the stream
	//
        if(destinationStreams[streamId]==null) return true;
	destinationStreams[streamId].putControlElementUpStream(controlElement);

	// Continue with the operator
	//
	return true;
    }


    /**
     * This function puts a control element to all destination streams that
     * have not been previously closed
     *
     * @param controlElement The control element to be sent
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted
     *            during execution
     * @exception NullElementException controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to put a control
     *            element in a previously closed stream
     */

    protected final boolean putControlElementToDestinationStreams(
				       StreamControlElement controlElement)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Loop over all destination streams and put the control
	// element in all open ones
	//
	int numDestinationStreams = getNumDestinationStreams();

	for (int dest = 0; dest < numDestinationStreams; ++dest) {

	    if (getDestinationStreamStatus(dest) !=
		DestinationStreamStatus.Closed) {

		boolean proceed = 
		    putControlElementToDestinationStream(controlElement, dest);

		if (!proceed) return false;
	    }
	}

	// Continue with the operator
	//
	return true;
    }


    /**
     * This function puts a control element to a destination stream with
     * priority
     *
     * @param controlElement The control element to be put
     * @param streamId The destination stream to which the control element
     *                 is to be put
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception NullElementException controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to put controlElement
     *            in a previously closed stream
     */

    protected final boolean priorityPutControlElementToDestinationStream (
					  StreamControlElement controlElement,
					  int streamId) 
	throws NullElementException,
	       StreamPreviouslyClosedException {

	// Put the control element to the destination stream with priority
	//
        //if(destinationStreams[streamId]==null) return true;
	destinationStreams[streamId].priorityPutControlElementUpStream(
							     controlElement);

	// Continue with the operator
	//
	return true;
    }


    /**
     * This function puts a control element with priority to all destination
     * streams that have not been previously closed
     *
     * @param controlElement The control element to be put
     *
     * @return True if the operator is to continue and false otherwise
     *
     * @exception java.lang.InterruptedException Thread is interrupted
     *            during execution
     * @exception NullElementException controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to put a control
     *            element in a previously closed stream
     */

    protected final boolean priorityPutControlElementToDestinationStreams(
				       StreamControlElement controlElement)
	throws NullElementException,
	       StreamPreviouslyClosedException {

	// Loop over all destination streams and put the control
	// element in all open ones
	//
	int numDestinationStreams = getNumDestinationStreams();

	for (int dest = 0; dest < numDestinationStreams; ++dest) {

	    if (getDestinationStreamStatus(dest) !=
		DestinationStreamStatus.Closed) {

		boolean proceed = 
		    priorityPutControlElementToDestinationStream(controlElement,
								 dest);

		if (!proceed) return false;
	    }
	}

	// Continue with the operator
	//
	return true;
    }


    ////////////////////////////////////////////////////////////////////
    // The following functions provide the hooks to write actual      //
    // operator classes. This is done by deriving from this class and //
    // over-riding the following operators.                           //
    ////////////////////////////////////////////////////////////////////

    /**
     * This function initializes the data structures for an operator
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean initialize () {

	// Currently, no initialization and the operator continues
	//
	return true;
    }


    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a non-blocking state.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement tupleElement,
						 int streamId,
						 ResultTuples result) {

	// By default does nothing and asks the operator to continue
	//
	return true;
    }


    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a blocking state.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean blockingProcessSourceTupleElement (
						 StreamTupleElement tupleElement,
						 int streamId) {

	// By default does nothing and asks the operator to continue
	//
	return true;
    }


    /**
     * This function returns the current output of the operator. This
     * function is invoked only when the operator is blocking.
     *
     * @param resultTuples The output array list to be filled with the
     *                     results computed and the destination streams
     *                     to which they are to be sent.
     * @param partial If this function call is due to a request for a
     *                partial result
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean getCurrentOutput (ResultTuples resultTuples, boolean partial) {

	// By default does nothing and asks the operator to continue
	//
	return true;
    }


    /**
     * This function clears the current output of the operator. This
     * function is invoked only when the operator is transitioning
     * from blocking to non-blocking.
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean clearCurrentOutput () {

	// By default does nothing and asks the operator to continue
	//
	return true;
    }


    /**
     * This function removes the effects of the partial results in a given
     * source stream.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     * @return True if the operator is to proceed and false otherwise.
     */

    protected boolean removeEffectsOfPartialResult (int streamId) {

	// Currently, no effects removed and the operator continues ...
	//
	return true;
    }


    /**
     * This function cleans up after the operator.
     */

    protected void cleanUp () {

	// Currently no clean up
	//
    }


    ///////////////////////////////////////////////////////////////////////
    // These functions can also be over-riden by the derived classes in  //
    // case a different propagation mechanism for control messages is    //
    // is required.                                                      //
    //                                                                   //
    // NOTE: In the realm of control message propagation, it is the      //
    // responsibility of this function to send shut down messages if any //
    // necessary to shut down other operators. Merely returning false to //
    // function will not sent shut down messages to other operators.     //
    ///////////////////////////////////////////////////////////////////////

    /**
     * This function propagates Shut Down control messages received from
     * a source stream.
     *
     * @param controlElement The shut down control element to be propagated
     * @param streamId The id of the source stream from which the control
     *                 element was read.
     *
     * @return True if the operator is to continue and false otherwise.
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception NullElementException If controlElement is null
     * @exception StreamPreviouslyClosedException Attempt to propagate
     *            a control element to a previously closed stream
     */

    protected boolean propagateSourceShutDownElement (
					StreamControlElement controlElement,
					int streamId) 
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Propagate the control element to all unclosed source streams but the
	// source stream from which it was read
	//
	int numSourceStreams = getNumSourceStreams();

	for (int src = 0; src < numSourceStreams; ++src) {

	    if (src != streamId &&
		getSourceStreamStatus(src) != SourceStreamStatus.Closed) {

		boolean proceed = putControlElementToSourceStream(controlElement,
								  src);
	    }
	}

	// Propagate the control element to all unclosed destination streams
	//
	boolean proceed = priorityPutControlElementToDestinationStreams(
							      controlElement);

	// Currently, always quit the operator
	//
	return false;
    }


    /**
     * This function propagates a GetPartialResult control element from a
     * destination stream
     *
     * @param controlElement The control element to be propagated
     * @param streamId The destination stream from which the control element
     *                 was read
     *
     * @return True if the operator is to proceed and false otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception NullElementException If controlElement is null
     * @exception StreamPreviouslyClosedException If an attempt is made to
     *            put a control element into a previously closed stream
     */

    protected boolean propagateDestinationGetPartialResultElement(
					  StreamControlElement controlElement,
					  int streamId)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Send the control element to all the open source streams
	//
	boolean proceed = putControlElementToSourceStreams(controlElement);

	// Return whether operator can continue or not
	//
	return proceed;
    }

    /*this function sets the data manager--Trigger*/
    public static void setDataManager(DataManager dm) {
	DM=dm;
    }
}    
