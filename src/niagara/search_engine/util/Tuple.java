
/**********************************************************************
  $Id: Tuple.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.util;

import java.util.*;

/**
 * data strucutre for result tuples.
 *
 *
 */
public class Tuple extends Vector implements Const {
    public Tuple() {
	super();
    }
    
    public Tuple(int size) {
	super();
	setSize(size);
    }
    
    public void merge(Tuple tup) {
	addAll(tup);
    }

    /*
    //for JDK1.1.6
    public void merge(Tuple tup) {
	for(int i=0; i<tup.size(); i++) {
	    addElement(tup.elementAt(i));
	}
    }
    */

    public Tuple getMerged(Tuple tup) {
      Tuple tp = new Tuple();
      for(int i=0; i<size(); i++) {
	tp.addElement(elementAt(i));
      }
      for(int i=0; i<tup.size(); i++) {
	tp.addElement(tup.elementAt(i));
      }
      return tp;
    }

  public String toString() {
    String result=""+elementAt(0);
    for (int i=1;i<size();i++) {
      result+=COLUMN_DELIMETER+elementAt(i);
    }
    return result;
  }
}
