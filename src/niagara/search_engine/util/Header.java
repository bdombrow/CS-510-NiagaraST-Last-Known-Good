
/**********************************************************************
  $Id: Header.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
 * data structure representing header of a result table.
 *
 *
 */
public class Header extends Vector implements Const {
    public Header() {
	super();
    }
    
    public Header(int size) {
	super();
	setSize(size);
    }
    
  public String getColumnName(int column) {
    return (String)elementAt(column);
  }
  public void setColumnName(int column, String name) {
    setElementAt(name, column);
  }
  public void addColumnName(String name) {
    addElement(name);
  }


    public void merge(Header header) {
	addAll(header);
    }

    /*
    //for JDK1.1.6
    public void merge(Header header) {
	for(int i=0; i<header.size(); i++) {
	    addElement(header.elementAt(i));
	}
    }
    */

    public Header getMerged(Header header) {
      Header hd = new Header();
      for(int i=0; i<size(); i++) {
	hd.addElement(elementAt(i));
      }
      for(int i=0; i<header.size(); i++) {
	hd.addElement(header.elementAt(i));
      }
      return hd;
    }

    /* for JDK1.2 */
  public String toString() {
    String result=getColumnName(0);
    for (int i=1;i<size();i++) {
      result+=COLUMN_DELIMETER+getColumnName(i);
    }
    return result;
  }

    /* for jdk1.1.* */
  /*
    public String toString2() {
	String result=getColumnName(0);
	for (int i=1;i<size();i++) {
	    result+=COLUMN_DELIMETER+getColumnName(i);
	}
	return result;
    }
  */
}
