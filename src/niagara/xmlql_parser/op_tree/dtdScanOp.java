
/**********************************************************************
  $Id: dtdScanOp.java,v 1.3 2002/05/07 03:11:27 tufte Exp $


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
 * This class represent dtd Scan operator that fetches the data sources,
 * parses it and then returns the DOM tree to the operator above it.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class dtdScanOp extends unryOp {

   private Vector docs;// Vector of urls to scan.
   private String type;// dtd name (e.g book.dtd)
   private Vector docsCpy = null; //trigger. the copy of the docs  

   /**
    * Constructor
    *
    * @param list of algorithm used to implement this operator
    */
   public dtdScanOp(Class[] al) {
	super(new String("DtdScan"), al);
   }
  
   /**
    * @return list of XML data sources (either URLs or local files)
    */
   public Vector getDocs() {
	return docs;
   }

   //trigger --Jianjun
   public Vector getDocsCpy() {
	return docsCpy;
   }

   //trigger --Jianjun
   public void makeDocsCpy() {
	String srcFileName;
	int size = docs.size();
	docsCpy = new Vector();
	for (int i=0; i<size; i++) {
		Object o = docs.elementAt(i);
		if (o instanceof data) {
			srcFileName=new String((String)((data)o).getValue());	
		}
		else {
			srcFileName = new String((String)o);
		}
		docsCpy.addElement(srcFileName);
	}
   }

   //trigger --Jianjun
   public void restoreDocs() {
	docs = docsCpy;
   }


    /**
     * This function sets the vector of documents associated with the
     * dtd scan operator
     *
     * @param docVector The set of documents associated with the operator
     */

    public void setDocs (Vector docVector) {

	docs = docVector;
    }

   /**
    * @return DTD type of the XML sources to query
    */
   public String getDtdType() {
	return type;
   }
  
   /**
    * sets the parameters for this operator
    *
    * @param list of XML data sources
    * @param DTD type of these sources
    */
   public void setDtdScan(Vector v, String s) {
	docs = v;
	type = s;
   }

   /**
    * prints this operator to the standard output
    */
   public void dump() {
      System.out.println("DtdScan :");
      for(int i=0;i<docs.size();i++) {
	 Object o = docs.elementAt(i);
	 if(o instanceof String)
	    System.out.println("\t"+(String)o);
	 else if(o instanceof data)
	    ((data)o).dump();
      }
      System.out.println("\t"+type);
   }

   /**
    * dummy toString method
    *
    * @return String representation of this operator
    */
   public String toString() {
      StringBuffer strBuf = new StringBuffer();
      strBuf.append("Data Scan");
      return strBuf.toString();
   }

    public String dumpChildrenInXML() {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < docs.size(); i++) {
            buf.append("<url value='" + docs.elementAt(i)  +"'/>");
        }
        return buf.toString();
    }
    
    public boolean isSourceOp() {
	return true;
    }
}
