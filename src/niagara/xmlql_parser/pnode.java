
/**********************************************************************
  $Id: pnode.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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
* This class is used for 'book keeping' in the generation of a chain of
* scan nodes in the logical plan generation
*
*
*/
package niagara.xmlql_parser;

public class pnode {
	private pattern pat;   // the pattern
	private int parent;    // pointer to the pattern representing the parent

	/**
	 * Constructor
	 *
	 * @param pattern
	 * @param index of the parent pattern
	 */

	public pnode(pattern _pat, int _parent) {
		pat = _pat;
		parent = _parent;
	}

	/**
	 * @return the pattern
	 */

	public pattern getPattern() {
		return pat;
	}

	/**
	 * @return the index of the parent
	 */

	public int getParent() {
		return parent;
	}
}
