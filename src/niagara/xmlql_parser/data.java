
/**********************************************************************
  $Id: data.java,v 1.1 2003/12/24 01:19:54 vpapad Exp $


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
 * This class is used to store value with its type (like IDEN, VAR, ATTR, etc.)
 *
 */
package niagara.xmlql_parser;

public class data {
    
    int type;       // type of data
    Object value;   // value of the data (String for IDEN and VAR, 
		    //                    schemaAttribute for ATTR)

    /**
     * Constructor
     *
     * @param type of the data value
     * @param value of the data
     */

    public data(int type, Object value) {
	this.type = type;
	this.value = value;
    }

    /**
     * @return type of the data value
     */
    public int getType() {
	return type;
    }

    /**
     * @return data value (String for IDEN and VAR, schemaAttribute for ATTR)
     */
    public Object getValue() {
	return value;
    }

    /**
     * @param value of this data object
     */
    public void setValue(Object o) {
	value=o;
    }


    /**
     * prints this object to the standard output
     */
    public void dump() {
	if(value instanceof String)
		System.out.println(type + (String)value);
	else if(type == dataType.ATTR)
		((schemaAttribute)value).dump();
    }

    /*This function is used to get a ASCII representation
    of the data. Currently only handle the very basic case.
    In the future, it needs to handle every case. --Jianjun */  

    public String toString() {
	if ((type == dataType.IDEN)||(type == dataType.STRING)) {	    
	    return (String)value;		
	}
	else {		
	    System.err.println("not supported yet--Trigger System");   
	    return null;
	}
    }

    /**
     * prints to the standard output
     *
     * @param number of tabs at the beginning of each line
     */
    public void dump(int depth) {
	for(int i = 0; i<depth; i++)
	System.out.print("\t");
	if(type == dataType.IDEN)
	    System.out.print("IDEN: ");
	else if(type == dataType.STRING)
	    System.out.print("STRING: ");
	else if(type == dataType.NUMBER)
	    System.out.print("NUMBER: ");
	else if(type == dataType.VAR)
	    System.out.print("VAR: ");
	if(value instanceof String)
		System.out.println("'" + (String)value+"'");
	else if(type == dataType.ATTR)
		((schemaAttribute)value).dump(depth);
    }

}
