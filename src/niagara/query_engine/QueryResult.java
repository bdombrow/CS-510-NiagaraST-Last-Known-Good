
/**********************************************************************
  $Id: QueryResult.java,v 1.13 2003/03/05 19:27:05 tufte Exp $


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

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.ndom.*;
import niagara.connection_server.NiagraServer;

/**
 * QueryResult is the class at the client that interacts with the
 * output stream of the query
 *
 * @version 1.0
 *
 */

public class QueryResult {

    /////////////////////////////////////////////////////////////
    // These are exceptions thrown by members of this class    //
    /////////////////////////////////////////////////////////////

    /**
     * This exception is thrown when an attempt is made to get a partial
     * result when a request is already pending
     */

    public class AlreadyReturningPartialException extends Exception {

    }


    /////////////////////////////////////////////////////////////
    // These are public static variables of this class         //
    /////////////////////////////////////////////////////////////

    /**
     * This return code implies that all the results have been returned
     */
    // public static final int EndOfResult = 0; // EOS

    /**
     * This return code implies that all partial result have been returned
     */
    //public static final int EndOfPartialResult = 1; //END_PARTIAL

    /**
     * This return code implies that there was an error in executing
     * the query
     */
    //public static final int QueryError = 2;

    /**
     * This return code implies that the query is non-blocking
     */
    // public static final int NonBlockingResult = 3; //SYNCH_PARTIAL

    /**
     * This return code implies the presence of a partial result
     */
    //public static final int PartialQueryResult = 4;

    /**
     * This return code implies the presence of a final result
     */
    //public static final int FinalQueryResult = 5;

    /**
     * This return code implies that a call timed out
     */
    //public static final int TimedOut = 6;


    ///////////////////////////////////////////////////////////////////
    // These are public nested classes                               //
    ///////////////////////////////////////////////////////////////////

    /**
     * This is a class for returning result to the user
     */

    public class ResultObject {
	public Document result; // the value read from stream
	public boolean isPartial; // is result partial or final
    }


    /////////////////////////////////////////////////////////////
    // These are the private data members of the class         //
    /////////////////////////////////////////////////////////////

    // The query id assigned by the query engine
    private int queryId;

    // The output stream to get results from
    private SourceTupleStream outputStream;

    // Variable to store whether the output stream is in the process
    // of generating possibly partial results
    private boolean generatingPartialResult;

    /**
     * This is the constructor that is initialized with information about
     * the query id and the output stream
     *
     * @param queryId The query id issued by the system
     * @param outputStream The output stream of the query
     */

    public QueryResult (int queryId, PageStream outputPageStream) {
	this.queryId = queryId;
	this.outputStream = new SourceTupleStream(outputPageStream);
	this.generatingPartialResult = false;
    }


    /**
     * This function returns the query id assigned by the query engine
     *
     * @return query id assigned by the query engine
     */

    public int getQueryId () {
	return queryId;
    }

    // KT - hack so I don't have to put ResultObject in it's own class,
    // as it is I'm removing about 10,000 extra allocations, so I get
    // one teeny weeny hack
    public ResultObject getNewResultObject() {
	return new ResultObject();
    }
    

    /**
     * This function blocks and waits for the next result from the output
     * stream
     *
     * @exception java.lang.InterruptedException If thread is interrupted during
     *            execution
     *
     * @returns Returns the control flag received from stream
     */
    public int getNextResult (ResultObject resultObject)
	throws java.lang.InterruptedException,
	       ShutdownException {
	// -1 indicates no timeout - infinite timeout
	return internalGetNext(-1, resultObject);
    }


    /**
     * This function waits for the next result from the output stream for
     * the specified timeout interval.
     *
     * @param timeout The specified timeout interval
     * @param resultObject An object to be filled in with the result
     *
     */
    public int getNextResult (int timeout, ResultObject resultObject)
	throws ShutdownException, InterruptedException {
	return internalGetNext(timeout, resultObject);
    }


    /**
     * This function request partial results to be sent
     *
     * @exception AlreadyReturningPartialException If a partial result
     *            request is pending
     */

    public void requestPartialResult ()
	throws AlreadyReturningPartialException,
	       ShutdownException {
		
	// If partial results are already invoked, 
	// then raise an exception
	if (generatingPartialResult) {
	    throw new AlreadyReturningPartialException();
	}
	
	// Send a request for a partial result		
	System.out.println("QR putting partial request down stream");
	// May return EOS, throw ShutdonwnEx or return NULLFLAG
	// Think I can ignore EOS...famous last words
	int ctrlFlag = outputStream.putCtrlMsg(CtrlFlags.GET_PARTIAL, null);

	assert (ctrlFlag == CtrlFlags.EOS || ctrlFlag == CtrlFlags.NULLFLAG) :
	    "Unexpected control flag";
	
	// Set the status of generating partial results
	generatingPartialResult = true;
    }


    /**
     * This function kills a query
     */

    public void kill() { 
	try {
	    // Attempt to kill the query - best effort - ignore errors

	    // return from outputStream.putCtrlMsg could be:
	    // NULLFLAG, SHUTDOWN, SYNCH_PARTIAL, END_PARTIAL, EOS
	    // can ignore all of them
	    outputStream.putCtrlMsg(CtrlFlags.SHUTDOWN, "kill query"); 
	} catch(ShutdownException e) {
	    // ignore since we are killing query...
	}
    }


    /**
     * send a request for buffer flush down stream - we've been waiting
     * too long for results
     *
     * @returns a result status, similar to what is in resultObject.status
     */
    public int requestBufFlush() 
	throws ShutdownException {
	return outputStream.putCtrlMsg(CtrlFlags.REQUEST_BUF_FLUSH, null);
    }

     
    /**
     * This function blocks and waits for the next result from the output stream.
     *
     * @param timeout Amount of time to sleep on query output stream
     * @param resultObject Object to be filled in with result
     *
     * @return control flag from stream
     *
     * @exception java.lang.InterruptedException If the thread was interrupted
     *            during execution. This happens only without a timeout
     */

    private int internalGetNext (int timeout, ResultObject resultObject) 
	throws InterruptedException,
	       ShutdownException {

	// Get the next element from the query output stream
	resultObject.result = null;
	
	if(timeout < 0) {
	    // neg or 0 implies no timeout, we use
	    // maxDelay, so we get tuples even from slow streams
	    timeout = PageStream.MAX_DELAY; 
	}

	StreamTupleElement tuple = outputStream.getTuple(timeout);	
	int ctrlFlag = outputStream.getCtrlFlag();	

	// Now handle the various types of results
	if (tuple ==  null) {
	    // process the control message
	    if(ctrlFlag == CtrlFlags.END_PARTIAL ||
	       ctrlFlag == CtrlFlags.SYNCH_PARTIAL) {
		assert generatingPartialResult : "Unexpected partial result";
		generatingPartialResult = false;
	    }
	} else {
	    assert ctrlFlag == CtrlFlags.NULLFLAG :
		"Unexpected control flag " + CtrlFlags.name[ctrlFlag];
	    resultObject.isPartial = tuple.isPartial();
	    resultObject.result = extractXMLDocument(tuple);
	} 
	return ctrlFlag;
    }

    private Document extractXMLDocument (StreamTupleElement tupleElement) {
	// First get the last attribute of the tuple
	Node lastAttribute = tupleElement.getAttribute(tupleElement.size() - 1);
	
	if(lastAttribute instanceof Document) {
	    return (Document)lastAttribute;
	} else if (lastAttribute instanceof Element) {
	    // Create a Document and add add the result to the Doc
	    Document resultDocument = DOMFactory.newDocument();
	    Node n = DOMFactory.importNode(resultDocument, 
					   lastAttribute);
	    resultDocument.appendChild(n);
	    return resultDocument;
	} else if (lastAttribute instanceof Attr) {
	    Document resultDocument = DOMFactory.newDocument();
	    // create an element from the attribute
	    Element newElt = 
		resultDocument.createElement(((Attr)lastAttribute)
					     .getName());
	    Text txt =
		resultDocument.createTextNode(((Attr)lastAttribute)
					      .getValue());
	    newElt.appendChild(txt);
	    resultDocument.appendChild(newElt);
	    return resultDocument;
	} else if (lastAttribute instanceof Text) {
	    Document resultDocument = DOMFactory.newDocument();
	    // create an element from the attribute
	    Element newElt = 
		resultDocument.createElement("Text");
	    Text txt =
		resultDocument.createTextNode(lastAttribute.getNodeValue());
	    newElt.appendChild(txt);
	    resultDocument.appendChild(newElt);
	    return resultDocument;
	} else {
	    throw new PEException("KT What did I get?? " +
				  lastAttribute.getClass().getName());
	}
    }
    

    public String toString() {
		return ("Query Result Object for Query "+queryId);
    }
}
