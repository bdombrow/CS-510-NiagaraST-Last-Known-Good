
/**********************************************************************
  $Id: patternInternalNode.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
* This class extends pattern and represents an internal node in the
* n-ary tree to represent patterns of the WHERE part
*
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

public class patternInternalNode extends pattern {
	
	private Vector patternList; // list of children (patterns)

	/**
	 * Constructor
	 *
	 * @param regular expression
	 * @param list of attributes
	 * @param list of children
	 * @param element_as or content_as variable
	 */

	public patternInternalNode(regExp re, Vector al, Vector pl, data bd) {
		super(re,al,bd);
		patternList = pl;
	}

	/**
	 * @return list of children
	 */
	public Vector getPatternList() {
		return patternList;
	}

	/**
	 * print to the standard output
	 *
	 * @param number of tabs at the beginning of each line
	 */

	public void dump(int i) {
		pattern child;
		super.dump(i);
		Util.genTab(i);
		System.out.println("sub-pattern");
		for(int j=0;j<patternList.size();j++) {
			child = (pattern)patternList.elementAt(j);
			child.dump(i+1);
		}
	}
}
