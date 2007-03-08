/**********************************************************************
  $Id: PunctSpec.java,v 1.1 2007/03/08 22:34:02 tufte Exp $


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
 * This class is used to represent a punctuation
 * specification.
 *
 */

package niagara.logical;

public class PunctSpec {
  
  public enum PunctType {
			 ONCHANGE, SOMETHINGELSE
      };
	  
	private PunctType pType;

	public PunctSpec(String punctSpec) {
		if(punctSpec.equalsIgnoreCase("ONCHANGE"));
			this.pType = PunctType.ONCHANGE;	
	}

  public PunctType getPunctType() {
		return pType;
	}

  public String toString() {
		switch(pType) {
			case ONCHANGE:
				return "On Change";
			default:
				assert false : "Invalid punctuation type";
        return null;
		}
  }	

  public int hashCode() {
    return pType.hashCode();
  }
}	

