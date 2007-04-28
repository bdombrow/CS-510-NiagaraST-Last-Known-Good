/**********************************************************************
  $Id: SimilaritySpec.java,v 1.2 2007/04/28 21:24:47 jinli Exp $


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
			  AllDays, WeekDays, SameDayOfWeek
      };
	  
	private SimilarityType sType;
	private int numOfDays;
	private int numOfMins;

	public SimilaritySpec(String type, int _numOfDays, int _numOfMins) {
    // do something smart
	if (type.compareToIgnoreCase("AllDays") == 0)
		sType = SimilarityType.AllDays;
	else if (type.compareToIgnoreCase("WeekDays") == 0)
		sType = SimilarityType.WeekDays;
	else if (type.compareToIgnoreCase("SameDayOfWeek")==0)
		sType = SimilarityType.SameDayOfWeek;
	else
		System.err.println("unsupported similarity type - "+type);
	
    numOfDays = _numOfDays;
    numOfMins = _numOfMins;
	}

  public SimilarityType getSimilarityType() {
		return sType;
	}
  
  public int getNumOfDays () {
	  return numOfDays;
  }

  public int getNumOfMins () {
	  return numOfMins;
  }
  
  public String toString() {
		switch(sType) {
			case AllDays:
				return "All days";
			case WeekDays:
				return "Week days";
			case SameDayOfWeek:
				return "Same day of week";
			default:
				assert false : "Invalid similarity type";
        return null;
		}
  }	
 
  public int hashCode() {
    return sType.hashCode();
  }



}	

