
/**********************************************************************
  $Id: Table.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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
 * data structure representing a result table.
 *
 *
 */
public class Table extends Vector {
    Header header = new Header();
    
    public Table() {
	super();
    }
    
    public boolean merge(Table tab) {
	int size=size();
	if (size>0 && size!=tab.size()) return false;
	int myColSize=colSize();
	int hisColSize=tab.colSize();
	for(int j=myColSize,k=0;j<myColSize+hisColSize;j++,k++) {
	    setColName(j, tab.getColName(k));
	}

	for(int i=0;i<size;i++) {
	    getTuple(i).merge(tab.getTuple(i));
	}
	return true;
    }
    
    public Tuple getTuple(int index) {
	return (Tuple)elementAt(index);
    }
    
    public void add(Tuple tup) {
	addTuple(tup);
    }

    public void addTuple(Tuple tup) {
	addElement(tup);
    }

    public void add(Table tab) {
      addTable(tab);
    }

    public void addTable(Table tab) {
        addAll(tab);
    }

    /*
    //for JDK1.1.6
    public void addTable(Table tab) {
	for(int i=0; i<tab.size(); i++) {
	    add(tab.getTuple(i));
	}
    }
    */
     
    public String getColName(int col) {
	return (String)header.elementAt(col);
    }

    public void setColName(int col, String name) {
	if (col >= header.size())
	    header.setSize(col+1);
	header.setElementAt(name, col);
    }

    public void addColName(String name) {
	header.addElement(name);
    }

    public int colSize() {
	return header.size();
    }
    
  public Header getHeader() {
    return header;
  }
   
    public String toString() {
	StringBuffer str = new StringBuffer();
	str.append(header);
	for (int i=0;i<size();i++) {
	  str.append("\n");
	  str.append(elementAt(i));
	}
	return str.toString();
    }

    public void printTable() {
	System.out.println(header);
	for (int i=0;i<size();i++) {
	  System.out.println(elementAt(i));
	}
    }
}
