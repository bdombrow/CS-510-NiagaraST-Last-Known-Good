
/**********************************************************************
  $Id: constructLeafNode.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
* This class is used for representing leaf node in the construct tree
*
*/
package niagara.xmlql_parser.syntax_tree;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class constructLeafNode extends constructBaseNode {
	private data leafData;

	/**
	 * constructor
	 *
	 * @param leaf data
	 */

	public constructLeafNode(data d) {
		super();
		leafData = d;
	}

	/**
	 * get the data
	 *
	 * @return leaf data
	 */

	public data getData() {
		return leafData;
	}

	/**
	 * if this leaf data is a variable then repalce it with the 
	 * schema attribute representing that variable
	 *
	 * @param variable table that maps variable to schema attribute
	 */

	public void replaceVar(varTbl vt) {

		schemaAttribute attr;
		int type = leafData.getType();
		if(type == dataType.VAR) {
			String var = (String)leafData.getValue();
			attr = vt.lookUp(var);
			if(attr == null) 
				System.out.println("this is not possible");
			leafData = new data(dataType.ATTR,attr);
		}
	}

	/**
	 * print leaf data to standard output
	 *
	 * @param number of tabs in the beginning of each line
	 */

	public void dump(int depth) {
		Util.genTab(depth);
		System.out.println("constructLeaf");
		leafData.dump(depth);
	}
}
