
/**********************************************************************
  $Id: inClause.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
* This class is used to represent the inClause
*    e.g.
*         <book>
*             <author> $a </>
*         </> IN "*" Conform_To xyz.dtd
*
*
*/
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

public class inClause extends condition {
	private pattern pat;   // pattern with the element names
	private Vector source; // source of data to query
	private String dtdType;// dtd type of documents to query (optional)

	/**
	 * Constructor
	 *
	 * @param pattern of the Where clause
	 * @param list of documents to query
	 * @param dtd type of the documents to query
	 */

	public inClause(pattern _pat, Vector _source, String _dtdType) {
		pat = _pat;
		source = _source;
		dtdType = _dtdType;
	}

	/**
	 * @return the pattern
	 */
	public pattern getPattern() {
		return pat;
	}

	/**
	 * @return the list of documents
	 */
	public Vector getSources() {
		return source;
	}

	/**
	 * @return dtd type of the documents
	 */
	public String getDtdType() {
		return dtdType;
	}

	/**
	 * display this to standard output
	 *
	 * @param number of tabs at the beginning of each line
	 */
	public void dump(int j) {
		System.out.println("IN CLAUSE");
		pat.dump(0);
		System.out.println("Source of Documents");
		for(int i=0; i<source.size(); i++)
			((data)source.elementAt(i)).dump(1);
		if(dtdType != null)
			System.out.println("ConformsTo: " + dtdType);
	}
}
