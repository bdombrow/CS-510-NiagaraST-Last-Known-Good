
/**********************************************************************
  $Id: UIDriverIF.java,v 1.2 2002/10/12 20:10:25 tufte Exp $


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
 * This interface is implemented by the GUI
 *
 */

package niagara.client;

public interface UIDriverIF {

    /** Notifies the UI that new results for query have arrived
     *  @param query id
     */
    public void notifyNew(int id);

    /** Notifies the UI that all the results for query have arrived
     *  @param query id
     */
    public void notifyFinalResult(int id);

    /** Displays error messages
     */
    public void errorMessage(int id, String err);
}
