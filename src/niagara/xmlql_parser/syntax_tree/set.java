
/**********************************************************************
  $Id: set.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
* This class is used to represent sets in XML-QL.
* e.g $A IN {author, editor}
*
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

public class set extends condition {
	private Vector set;     // list of identifiers (author and editor in
			        //        the above example)
	private String var;     // the set variable ($A in the above example)
	private regExp equivRE; // produced by ORing the identifiers
				// the above example will give :
				//             |
				//            / \
				//       author editor

	/**
	 * Constructor
	 *
	 * @param the name of the variable
	 * @param the list of identifiers
	 */

	public set(String _var, Vector _set) {
		set = _set;
		var = _var;
		equivRE = Util.getEquivRegExp(set);
	}

	/**
	 * @return the list of identifiers
	 */

	public Vector getSet() {
		return set;
	}

	/**
	 * @return the name of the variable
	 */

	public String getVar() {
		return var;
	}

	/**
	 * @return the equivalen regular expression
	 */

	public regExp getRegExp() {
		return equivRE;
	}

	/**
	 * print to the standard output
	 *
	 * @param number of tabs at the beginning of each line
	 */

	public void dump(int i) {
		System.out.println("SET");
		System.out.println("variable: " + var);
		System.out.println("equivalent regular expression");
		equivRE.dump(1);
	}
}
