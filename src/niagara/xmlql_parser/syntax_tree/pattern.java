
/**********************************************************************
  $Id: pattern.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * This class is used to represent the pattern of the InClause
 * e.g.
 *       <book>
 *          <author> $a </>
 *       </> ....
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

public class pattern {
    
     private regExp regularExp;	      // for the start tag
     private Vector attrList;	      // list of attributes (name, value) pair
     private data bindingData;	      // element_as or content_as

     /**
      * Constructor
      *
      * @param regular expression for tag name
      * @param list of attributes
      * @param element_as or content_as variable
      */

     public pattern(regExp re, Vector al, data bd) {
	regularExp = re;
	attrList = al;
	bindingData = bd;
     }

     /**
      * @return regular expression for the tag name
      */
     public regExp getRegExp() {
	return regularExp;
     }

     /**
      * @return list of attributes
      */
     public Vector getAttrList() {
	return attrList;
     }

     /**
      * @return variable representing content_as or element_as
      */
     public data getBindingData() {
	return bindingData;
     }

     /**
      * print to the standard output
      *
      * @param number of tabs at the beginning of each line
      */

     public void dump(int i) {
		if(regularExp instanceof regExpOpNode)
			((regExpOpNode)regularExp).dump(i);
		else
			((regExpDataNode)regularExp).dump(i);
		for(int j=0; j<attrList.size(); j++)
			((attr)attrList.elementAt(j)).dump(i);
		if(bindingData != null)
			bindingData.dump(i);
     }
}

