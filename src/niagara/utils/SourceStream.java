
/**********************************************************************
  $Id: SourceStream.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * This is the <code>SourceStream</code> class the provides the interface
 * for producers writing to streams.
 *
 * @version 1.0
 */

public class SourceStream {
	
    /////////////////////////////////////////////////////
    //   Data members of the SourceStream Class
    /////////////////////////////////////////////////////

    // The stream that is to be written to
    //
    Stream stream;
    

    ///////////////////////////////////////////////////
    //   Methods of the SourceStream Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the SourceStream class that initializes it
     * with the stream to be written to.
     *
     * @param stream The stream that is to be written to
     */
     
    public SourceStream (Stream stream) {

	// Call the constructor of the super class
	//
	super();

	// Set the stream that is to be written to
	//
	this.stream = stream;
    }
		     

    /**
     * This function writes an object into a stream.
     *
     * @param object The object to be written to the stream
     *
     * @return True if the producer is to continue; False otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted during
     *            execution
     * @exception NullElementException If object is null
     * @exception StreamPreviouslyClosedException Attempt to write onto a
     *            previously closed stream
     */

    public boolean steput(StreamTupleElement tuple)
        throws java.lang.InterruptedException,
    NullElementException,
    StreamPreviouslyClosedException {
        boolean done = false;
        do {
            // Try to put tuple in the stream
            //
            StreamControlElement controlElement = 
                stream.putTupleElementUpStream(tuple);

            // If there is a control element, process it
            //
            if (controlElement != null) {

                if (controlElement.type() == StreamControlElement.ShutDown) {

                    // A shut down - so quit
                    //
                    return false;
                }

                // Ignore other control messages
                //
            }
            else {

                // The put was successful
                //
                done = true;
            }
        } while (!done);

        // Tuple was successfully put
        //
        return true;
    }

    public boolean put (Object object) 
        throws java.lang.InterruptedException,
    NullElementException,
    StreamPreviouslyClosedException {

        // First create a tuple that is not a partial result
        //
        StreamTupleElement tuple = new StreamTupleElement(false, 1);

        // Add the object as an attribute of the tuple
        //
        tuple.appendAttribute(object);

        // Repeatedly try to put tuple in the stream
        //
        boolean done = false;

        do {
            // Try to put tuple in the stream
            //
            StreamControlElement controlElement = 
                stream.putTupleElementUpStream(tuple);

            // If there is a control element, process it
            //
            if (controlElement != null) {

                if (controlElement.type() == StreamControlElement.ShutDown) {

                    // A shut down - so quit
                    //
                    return false;
                }

                // Ignore other control messages
                //
            }
            else {

                // The put was successful
                //
                done = true;
            }
        } while (!done);

        // Tuple was successfully put
        //
        return true;
    }


    /**
     * This function closes the stream written to
     *
     * @exception StreamPreviouslyClosedException Attempt to close a previously
     *            closed stream
     */

    public void close () throws StreamPreviouslyClosedException {

        // Close the stream
        //
        stream.close();
    }


    public String toString(){
        return stream.toString();
    }
}
