
/**********************************************************************
  $Id: StreamElementEnvelope.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * This class serves as an envelope for <code>StreamElement</code>s traveling accross the 
 * network. The information accompanying the stream elements can be useful in multiplexing 
 * more than one network stream on a connection
 *
 * @version 0.0
 */

import java.io.*;

class StreamElementEnvelope implements java.io.Serializable
{
    // Constants indicating the priority of
    // a control element sent up stream
    public static final int HIGH = 1;
    public static final int LOW = 0;
    
    // Data members
    
    // The connection ID of the network stream this 
    // Tuple Element originates from
    private NetworkStreamConnectionID connectionID;
    
    // The priority of the control element inside this
    // envelope. The default is LOW and it is only checked
    // on the NetworkUpperEndStream side.
    private int priority = LOW;
    
    // The Stream element piggy backed to the other side 
    private Object streamElement = null;

    // This flag indicates whether the object gotten is the initial object
    private boolean initialObject = false;
   
    /**
     * Constructor No 1.
     * @param element The element to be piggybacked to the other side
     * @param ID The connection ID of the sending object
     */
    StreamElementEnvelope(StreamElement element, NetworkStreamConnectionID ID)
	{
	    streamElement = element;
	    connectionID = ID;
	    initialObject = false;
	}
    
    /**
     * Constructor No 2.
     * @param element The element to be piggybacked to the other side
     * @param ID The connection ID of sending object
     * @param priority The priority of the Control Element. This will get it to the right buffer on the other side
     */
    StreamElementEnvelope(StreamElement element, NetworkStreamConnectionID ID, int priority)
	{
	    streamElement = element;
	    connectionID = ID;
	    this.priority = priority;
	    initialObject = false;
	}
    
    /**
     * Constructor No 3. This constructs a stream element envelope containing the initial object 
     * to be sent to the other end (there where the lower end stream resides). This constructor
     * should only be used inside the NetworkUpperEndStream constructor.
     */
    StreamElementEnvelope(Object o, NetworkStreamConnectionID ID)
	{
	    streamElement = o;
	    connectionID = ID;
	    initialObject = true;
	}
    
    /**
     * Get the priority
     *
     * @return Either StreamElementEnvelope.HIGH or StreamElementEnvelope.LOW
     */
    int getPriority()
	{
	    return priority;
	}
    
    /**
     * Get the ID of the sender
     *
     * @return The senders id object
     */
    NetworkStreamConnectionID getID()
	{
	    return connectionID;
	}
    
    /**
     * Get the "desired" stream element
     *
     * @return The shipped stream element
     */
    StreamElement getStreamElement()
	{
	    return (StreamElement)streamElement;
	}

    /**
     * This shows whether the this envelope carries an initial object
     *
     * @return true if the envelope carries an initial object
     */
    boolean isInitialObject()
	{
	    return initialObject;
	}
    

    /** 
     * Get the inital object if this is what this stream is 
     * carrying along.
     *
     * @return The initial object sent from the upper end to the lower end
     */
    Object getInitialObject()
	{
	    if(initialObject){
		return streamElement;
	    } else {
		System.err.println("This is a programming error. getInitialObject is only called after a check on the isInitialObject variable");
		return null;
	    }	    
	}
    
}




