
/**********************************************************************
  $Id: SortOp.java,v 1.1 2000/08/21 00:38:37 vpapad Exp $


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
 * This class is used to represent the Sort operator
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class SortOp extends unryOp {
    // Constants used for comparison method
    public final static short ALPHABETIC_COMPARISON = 0;
    public final static short  NUMERIC_COMPARISON = 1;

    /**
     * Compare as numbers or strings
     */
    private short comparisonMethod;

    private boolean ascending;


    /**
     * The attribute we are sorting on
     */
    private schemaAttribute attr;

   /**
    * Constructor
    *
    * @param list of algorithm to implement this operator
    */
   public SortOp(Class[] al) {
	super("Sort", al);
   }

   /**
    * @return the comparison method
    */
   public short getComparisonMethod() {
	return comparisonMethod;
   }

   /**
    * @return if the sort is ascending
    */
   public boolean getAscending() {
       return ascending;
   }

    /**
     * 
     * @return the attribute we are sorting on
     */
    public schemaAttribute getAttr() {
	return attr;
    }

   /**
    * used to configure the comparison method, and whether the
    * sort is ascending or not
    */
   public void setSort(schemaAttribute attr, short comparisonMethod, boolean ascending) {
       this.attr = attr;
       this.comparisonMethod = comparisonMethod;
       this.ascending = ascending;
   }

   /**
    * print the operator to the standard output
    */
   public void dump() {
      System.out.println("Sort");
   }

   /**
    * dummy toString method
    *
    * @return String representation of this operator
    */
   public String toString() {
       return "Sort";
   }
}

