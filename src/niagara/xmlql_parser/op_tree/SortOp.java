
/**********************************************************************
  $Id: SortOp.java,v 1.7 2003/07/08 02:11:06 tufte Exp $


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

import org.w3c.dom.Element;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

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
    private Attribute attr;

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
    public Attribute getAttr() {
	return attr;
    }

   /**
    * used to configure the comparison method, and whether the
    * sort is ascending or not
    */
   public void setSort(Attribute attr, short comparisonMethod, boolean ascending) {
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
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return input[0].copy();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
       SortOp op = new SortOp();
       op.setSort(attr, comparisonMethod, ascending);
       return op;
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SortOp)) return false;
        if (obj.getClass() != SortOp.class) return obj.equals(this);
        SortOp other = (SortOp) obj;
        return comparisonMethod == other.comparisonMethod &&
               ascending == other.ascending &&
               attr.equals(other.attr);
    }

    public int hashCode() {
        int result = comparisonMethod ^ attr.hashCode();
        if (ascending) result = ~result;
        return result;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        LogicalProperty inputLogProp = inputProperties[0];

        String sortbyAttr = e.getAttribute("sort_by");
        Attribute sortBy = Variable.findVariable(inputLogProp, sortbyAttr);

        short comparisonMethod;
        String comparisonAttr = e.getAttribute("comparison");
        if (comparisonAttr.equals("alphabetic"))
            comparisonMethod = SortOp.ALPHABETIC_COMPARISON;
        else
            comparisonMethod = SortOp.NUMERIC_COMPARISON;

        boolean ascending;
        String orderAttr = e.getAttribute("order");
        ascending = !orderAttr.equals("descending");
        setSort(sortBy, comparisonMethod, ascending);
    }
}

