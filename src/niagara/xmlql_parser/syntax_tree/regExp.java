
/**********************************************************************
  $Id: regExp.java,v 1.2 2000/08/21 00:41:04 vpapad Exp $


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
 *
 * Regular Expression is represented as a binary tree. There are two type
 * of nodes: one represent the operators (like *, +, etc.) while other 
 * represents data (string or variable). This is the base class for both
 * of these types - regExpDataNode and regExpOpNode
 *
 */
package niagara.xmlql_parser.syntax_tree;


import org.w3c.dom.*;

public abstract class regExp {

    public abstract void dump(int depth);
    public abstract String toString();
                    
    public void genTab(int depth){
	for(int i=0; i<depth; i++)
	    System.out.print("\t");
    }
}
