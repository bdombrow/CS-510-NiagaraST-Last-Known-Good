
/**********************************************************************
  $Id: StreamControlElement.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * This is the <code>StreamControlElement</code> class that is the unit
 * of transfer of control elements across a stream.
 *
 * @version 1.0
 *
 * @see Stream
 */


public final class StreamControlElement extends StreamElement {

    // Constants the specify the type of control element
    //
    public final static int ShutDown = 0;
    public final static int GetPartialResult = 1;
    public final static int SynchronizePartialResult = 2;
    public final static int EndPartialResult = 3;
    

    ///////////////////////////////////////////////////
    //   Data members of the StreamControlElement Class
    ///////////////////////////////////////////////////

    // The type of control element
    //
    private int element_type;


    ///////////////////////////////////////////////////
    //   Methods of the StreamControlElement Class
    ///////////////////////////////////////////////////

    /*
     * The constructor that initializes the stream control element with
     * a type
     *
     * @param type The type of the control element
     */

    public StreamControlElement (int type) {
	
	this.element_type = type;
    }


    /**
     * This function returns the type of the control element
     *
     * @return The type of the control element
     */

    public int type () {

	return element_type;
    }

    
    /**
     * This function returns true if a stream element is a stream control element
     * of a particular type. It returns false otherwise.
     *
     * @param streamElement the stream element whose type is to be checked.
     * @param type the type of control element to be checked for
     *
     * @return true if streamElement is a control element of desired type. False
     *         otherwise.
     */

    public static boolean isOfType (StreamElement streamElement, int type) {

	// First check whether it is a control element
	//
	if (streamElement instanceof StreamControlElement) {

	    StreamControlElement controlElement = 
		                 (StreamControlElement) streamElement;

	    // Now check whether it is of appropriate type
	    //
	    if (controlElement.type() == type) {

		return true;
	    }
	}

	return false;
    }
}
