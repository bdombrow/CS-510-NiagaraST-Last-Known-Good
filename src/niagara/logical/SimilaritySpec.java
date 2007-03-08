/**********************************************************************
  $Id: SimilaritySpec.java,v 1.1 2007/03/08 22:34:02 tufte Exp $


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
 * This class is used to represent a similarity 
 * specification.
 *
 */
package niagara.logical;

public class SimilaritySpec {
  

  public enum SimilarityType {
			  SOMETHING, SOMETHINGELSE
      };
	  
	private SimilarityType sType;

	public SimilaritySpec(String sSpecStr) {
    // do something smart
    sType = SimilarityType.SOMETHING;
	}

  public SimilarityType getSimilarityType() {
		return sType;
	}

  public String toString() {
		switch(sType) {
			case SOMETHING:
				return "something";
			default:
				assert false : "Invalid similarity type";
        return null;
		}
  }	
 
  public int hashCode() {
    return sType.hashCode();
  }



}	

