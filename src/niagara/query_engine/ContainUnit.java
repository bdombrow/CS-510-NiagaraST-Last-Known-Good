
/**********************************************************************
  $Id: ContainUnit.java,v 1.2 2003/12/24 01:31:49 vpapad Exp $


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


package niagara.query_engine;

import java.util.*;

import niagara.xmlql_parser.*;

/**
 * A ContainUnit corresponds to a SchemaUnit. Unlike SchemaUnit which
 * records a parent pointer, a ContainUnit records a list of children
 * pointers, as well as selection predicates
 */
class ContainUnit {
    private regExp tagExp;
    private Vector children; // of Integer's
    private int parentIndex = -1;
    private boolean usedInConstructTree = false;

    public ContainUnit (regExp tag, int parent) {
	tagExp = tag;
	children = new Vector();
	parentIndex = parent;
    }

    public void addChild (int index) {
	children.addElement (new Integer(index));
    }

    public regExp getTagExpression() {
	return tagExp;
    }

    public int numChildren() {
	return children.size();
    }

    public int getChild (int i) {
	return ((Integer)children.elementAt(i)).intValue();
    }

    public Vector getChildren() {
	return children;
    }

    public int getParent () {
	return parentIndex;
    }

    public boolean isUsedInConstructTree () {
	return usedInConstructTree;
    }

    public void setUsedInConstructTree() {
	usedInConstructTree = true;
    }

	public void dump() {
		System.out.print ("regexp:");
		if (tagExp == null) System.out.println ("NULL");
		else if (tagExp instanceof regExpDataNode) {
			System.out.print ("regExpDataNode: ");
			tagExp.dump(0);
		}
		else if (tagExp instanceof regExpOpNode) {
			System.out.print ("regExpOpNode: ");
			tagExp.dump(0);
		}

		System.out.println ("children: "+children);
		System.out.println ("parent: "+parentIndex);
	}
}

