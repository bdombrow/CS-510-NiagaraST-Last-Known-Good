
/**********************************************************************
  $Id: DMClosedException.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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



/**
 * DMClosedException.java
 *
 *
 * Created: Thu Apr 22 13:08:55 1999
 *
 * @version
 */

package niagara.data_manager;
/**
 * data manager is being closed, any call to data manager during this
 * period would result in a DMClosedException
 *
 * @see DMException
 */
public class DMClosedException extends DMException {

    /**
     * constructor
     * @see DMException
     */
    public DMClosedException() {
        super("Data Manager closed");
    }
    
} // DMClosedException
