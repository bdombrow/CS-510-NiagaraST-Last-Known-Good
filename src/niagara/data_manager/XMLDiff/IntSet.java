
/**********************************************************************
  $Id: IntSet.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


package niagara.data_manager.XMLDiff;

import java.util.*;


public class IntSet {
    final int SET_SIZE = 30;
    final double OK_RATIO = 0.2;
    int level;
    int size;
    Hashtable set;
    
    public IntSet() {
	level = 1;
	size = 0;
	set = new Hashtable();
    }

    public void insert(int toadd) {
	while(size==SET_SIZE) {
	    level *= 2;
	    Enumeration enum = set.keys();
	    Integer tmp;
	    while(enum.hasMoreElements()) {
		tmp = (Integer)enum.nextElement();
		if(tmp.intValue() % level != 0) {
		    Integer val = (Integer)set.get(tmp);
		    size -= val.intValue();
		    set.remove(tmp);
		}
	    }
	}

	if( (toadd % level)!=0 ) return;
	
	Integer ToAdd = new Integer(toadd);
	if(set.containsKey(ToAdd)) {
	    Integer val = (Integer)set.get(ToAdd);
	    set.remove(ToAdd);
	    set.put(ToAdd, new Integer(val.intValue()+1));
	}
	else set.put(ToAdd, new Integer(1));

	size++;
    }		    
	
    public void merge(IntSet other) {
	Hashtable s = other.set;
	Enumeration enum = s.keys();
	
	while(enum.hasMoreElements()) {
	    Integer tmp = (Integer)enum.nextElement();
	    int t = tmp.intValue();
	    if(t%level != 0) continue;
	    else {
		int c = ((Integer)(s.get(tmp))).intValue();
		while(c>0) {
		    insert(t);
		    c--;
		}
	    }
	}
    }

    public boolean goodMatch(IntSet other) {
	int diff=0;

	Hashtable s1 = (Hashtable)set.clone();
	Hashtable s2 = (Hashtable)other.set.clone();
	int sz1 = size;
	int sz2 = other.size;

        int common = 0;
  	if(level<=other.level) {
	    Enumeration enum1 = s1.keys();
	    Integer tmp;
	    while(enum1.hasMoreElements()) {
		tmp = (Integer)enum1.nextElement();
		if(tmp.intValue() % other.level != 0 || !s2.containsKey(tmp)) {
		    // Integer val = (Integer)s1.get(tmp);
		    // sz1 -= val.intValue();
		    // s1.remove(tmp);
		}
                else {
                    int v1 = ((Integer)s1.get(tmp)).intValue();
                    int v2 = ((Integer)s2.get(tmp)).intValue();
                    common += v1 < v2 ? v1 : v2;
                }
	    }
	}
	else {
	    Enumeration enum2 = s2.keys();
	    Integer tmp;
            while(enum2.hasMoreElements()) {
		tmp = (Integer)enum2.nextElement();
		if(tmp.intValue() % other.level != 0 || !s1.containsKey(tmp)) {
		    // Integer val = (Integer)s1.get(tmp);
		    // sz1 -= val.intValue();
		    // s1.remove(tmp);
		}
                else {
                    int v1 = ((Integer)s1.get(tmp)).intValue();
                    int v2 = ((Integer)s2.get(tmp)).intValue();
                    common += v1 < v2 ? v1 : v2;
                }
	    }
	}
        System.err.println("Common ele " + common);
        System.err.println("Size1 " + sz1);
        System.err.println("Size2 " + sz2);
	double ratio = ((double)common+common)/((double)(sz1+sz2));
	
	if(ratio > OK_RATIO) return true;
	else return false;
    }
}








