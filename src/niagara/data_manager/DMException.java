
/**********************************************************************
  $Id: DMException.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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
 * DMException.java
 * Created: Thu Apr 22 13:04:59 1999
 *
 * @version
 */

package niagara.data_manager;
/**
 * base class for all exceptions in data manager
 * @see Exception
 */
public class DMException extends Exception {
  
    /**
     * constructor
     * @see Exception
     */
    public DMException() {  
        super("Data Manager Exception:");
    }
    
    /**
     * constructor
     * @param msg the exception message
     * @see Exception
     */
    public DMException(String msg) 
    {
        super("Data Manager Exception:"+msg);
        
    }
    
} // DMException
