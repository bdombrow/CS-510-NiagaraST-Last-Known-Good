
/**********************************************************************
  $Id: Schema.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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
 * This class is used to describe the tuples generated during query processing.
 * It also helps in the generation of logical plan by capturing the parent-child
 * relationship among the elements scanned. The variables in the predicates are
 * replaced by attribute which has the position of the element in the Schema.
 *
 *
 */
package niagara.xmlql_parser;

import java.util.*;

public class Schema {
	private Vector tupleDes;  //  Vector of SchemaUnits

   // Constructor
   public Schema() {
	tupleDes = new Vector();
   }


    // constructor from an existing schema
    public Schema(Schema sc) {
	tupleDes = new Vector(sc.getVector());
    }

   /**
    * This function gives the depth of a Nth schema unit
    *
    * @param nth element whose depth from the top element has to be calculated
    * @return depth from the top element
    *
    */

   public int level(int i) {
	int depth = 0;
	int j;
	if(i==0)
		return -1;
	SchemaUnit currUnit = (SchemaUnit)tupleDes.elementAt(i);
	while((j = currUnit.getBackPtr()) != 0) {
		depth++;
		currUnit = (SchemaUnit)tupleDes.elementAt(j);
	}
        return depth;
   }

   /**
    * to add schemaunit at the end of the schema
    *
    * @param schemaunit to add
    */

   public void addSchemaUnit(SchemaUnit su) {
	tupleDes.addElement(su);
   }

   /**
    * to return the ith schemaunit
    *
    * @param position of the schemaunit to be returned
    */

   public SchemaUnit getSchemaUnit(int i) {
	return (SchemaUnit)tupleDes.elementAt(i);
   }

   /**
    * @return the number of schemaunits in this schema
    */

   public int numAttr() {
	return tupleDes.size();
   }

   /**
    * @return the vector representing the schema
    */

   public Vector getVector() {
	return tupleDes;
   }

   /**
    * dumps the schema on the standard output
    */

   public void dump() {
      System.out.println("Schema :");
      for(int i=0;i<tupleDes.size();i++)
	 ((SchemaUnit)tupleDes.elementAt(i)).dump();
      System.out.println("---------------------------------");
   }
}

