
/**********************************************************************
  $Id: StreamEosAcknowledgeElement.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * This is the <code>StreamEosAcknowledgeElement</code> class that indicates the
 * acknowledgement by the NetworkUpperEndStream of a StreamEosElement sent to it 
 * by the a NetworkLowerEndStream object. This Stream element is used only inside
 * the Network Streams and should never be exposed to the physical operators accessing
 * the streams.
 *
 * @version 1.0
 * 
 *
 * @see NetworkUpperEndStream, NetworkLowerEndStream
 */


public final class StreamEosAcknowledgeElement extends StreamElement {
    // nothing in here ...
}

