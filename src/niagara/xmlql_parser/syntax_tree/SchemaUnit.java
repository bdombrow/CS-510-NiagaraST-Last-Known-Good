
/**********************************************************************
  $Id: SchemaUnit.java,v 1.3 2003/07/08 02:11:05 tufte Exp $


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
 * This class is used to construct Schema for describing tuples during the
 * generation of logical plan tree. Corresponds to attributes of a Schema in
 * a traditional relational database.
 *
 */
package niagara.xmlql_parser.syntax_tree;

public class SchemaUnit {
    private regExp regexp=null;    // Describes Tags
    private int index=-1;          // back pointer to enclosing element

    /**
     * constructor 
     * @param regular expression and parent index
     **/

    public SchemaUnit (regExp reg, int ind) {
	regexp=reg;
	index=ind;
    }

    /**
     * constructor
     * @param another schemaunit to make a copy of (uses same regular 
     *            expression )
     */

    public SchemaUnit (SchemaUnit su) {
	regexp = su.regexp;
	index = su.index;
    }

    /**
     * get the regular expression 
     *
     * @return regexp 
     **/

    public regExp getRegExp() 
    {
	return(regexp);
    }
    
    /**
     * get the parent index 
     *
     * @return index 
     **/
    public int getIndex() 
    {
	return(index);
    }   

    /**
     * same as getIndex()
     *
     * @return index of the parent
     */

    public int getBackPtr() {
	return index;
    }

    /**
     * prints the schemaunit to the standard output
     */

    public void dump() {
       System.out.println("SchemaUnit :");
       if(regexp == null)
	  System.out.println("NULL");
       else
          regexp.dump(0);
       System.out.println("parent : " + index);
    }
}

