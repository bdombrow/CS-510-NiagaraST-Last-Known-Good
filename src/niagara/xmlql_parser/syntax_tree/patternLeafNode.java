
/**********************************************************************
  $Id: patternLeafNode.java,v 1.2 2002/10/26 21:57:11 vpapad Exp $


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
* represents leaf of a pattern tree (iden/var/null)
*
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

import niagara.logical.*;

public class patternLeafNode extends pattern {

	private data expData;  // leaf of the pattern

	/**
	 * Constructor
	 *
	 * @param regular expression for tags
	 * @param list of attributes
	 * @param leaf data (identifier or variable)
	 * @param element_as or content_as var
	 */

	public patternLeafNode(regExp re, Vector al, data ed, data bd) {
		super(re,al,bd);
		expData = ed;
	}

	/**
	 * Constructor
	 *
	 * @param regular expression for tags
	 * @param list of attributes
	 * @param element_as or content_as var
	 */

	public patternLeafNode(regExp re, Vector al, data bd) {
		super(re,al,bd);
	}

        /** Constructor that uses the new predicate data structures */
        public patternLeafNode(regExp re, Vector al, Atom ed, data bd) {
            super(re,al,bd);
            if (ed.isVariable()) {
                expData = new data(dataType.VAR, ((Variable) ed).getName());
            }
            else {
                expData = new data(dataType.IDEN, ((StringConstant) ed).getValue());
            }
        }

	/**
	 * @return leaf data
	 */
	public data getExpData() {
		return expData;
	}

	/**
	 * displays on the satandard output
	 *
	 * @param number of tabs at the beginning of each line
	 */
	public void dump(int i) {
		super.dump(i);
		expData.dump(i);
	}
}
