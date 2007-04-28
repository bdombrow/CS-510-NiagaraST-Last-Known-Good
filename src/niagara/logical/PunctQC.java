/**********************************************************************
  $Id: PunctQC.java,v 1.2 2007/04/28 21:29:55 jinli Exp $


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
 * This class is used to represent the punctqc operator
 * which punctuates a stream and also does query control.
 *
 */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.xmlql_parser.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class PunctQC extends BinaryOperator {
  
	private PunctSpec pSpec;
    private SimilaritySpec sSpec;
    private PrefetchSpec pfSpec;
    private String queryString;
    
    //The attribute we are punctuating on
    private Attribute pAttr;
    
    // The attribute of the input stream that we rely on to 
    // retrieve data from db - stream punctuating attr
    private Attribute spAttr;
    
	//The data value corresponding to the timer value     
    private String timeAttr;

    //private Attribute attrDataTimer;

    public PunctQC() {
    }

    // attribute we are punctuating on, a punctuation
    // specification, and a query control specification
    public PunctQC(Attribute pAttr, String timeAttr, PunctSpec pSpec, 
        SimilaritySpec sSpec, PrefetchSpec pfSpec, String queryString) {
			this.pAttr = pAttr;
			this.pSpec = pSpec;
			this.sSpec = sSpec;
			this.pfSpec = pfSpec;
			this.timeAttr = timeAttr;
			this.queryString = queryString;
    }

    public void setSPAttr (Attribute spAttr) {
    	this.spAttr = spAttr;
    }
    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println(toString());
    }

    /**
     * dummy toString method
     *
     * @return the String representation of the operator
     */
    public String toString() {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("PunctQC: Punct: " + pSpec.toString() +
						          " Attr: " + pAttr.getName());
        strBuf.append("Similarity: " + sSpec.toString()); 
        strBuf.append("Prefetching: " + pfSpec.toString());
        strBuf.append("Stream Punctuating Attr: "+spAttr.getName());
        return strBuf.toString();
    }

    public Attribute getPunctAttr() {
			return pAttr;
    }
    
    public PunctSpec getPunctSpec() {
			return pSpec;
    }
   
    public SimilaritySpec getSimilaritySpec() {
			return sSpec;
    }
    
    public PrefetchSpec getPrefetchSpec() {
			return pfSpec;
    }
    
    public Attribute getStreamPunctAttr() {
    	return spAttr;
    }

    public void dumpAttributesInXML(StringBuffer sb) {
			assert false : "Not implemented";
    }

    public String getQueryString() {
    	return queryString;
    }
    
    public String getTimeAttr() {
    	return timeAttr;
    }
    
    public void dumpChildrenInXML(StringBuffer sb) {
				assert false : "Not implemented";
        sb.append(">");

        sb.append("</punctuate>");
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {

					// The output schema is exactly the schema of the data input
					LogicalProperty result = input[0].copy();
				  return result;
    }

    public Op opCopy() {
        PunctQC other =  new PunctQC(this.pAttr, this.timeAttr, this.pSpec, this.sSpec, this.pfSpec, this.queryString);
        other.setSPAttr(this.spAttr);
        return other;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof PunctQC))
            return false;
        if (obj.getClass() != PunctQC.class)
            return obj.equals(this);
        PunctQC op = (PunctQC) obj;
        return pAttr.equals(op.pAttr) &&
                   pSpec.equals(op.pSpec) &&
                   sSpec.equals(op.sSpec) && 
                   pfSpec.equals(op.pfSpec) &&
                   spAttr.equals(op.spAttr);
    }

    public int hashCode() {
        return pAttr.hashCode() ^ pSpec.hashCode() ^ sSpec.hashCode() ^ pfSpec.hashCode() ^ spAttr.hashCode();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        String id = e.getAttribute("id");

				String punctAttrStr = e.getAttribute("punctattr");
				if (punctAttrStr.length() == 0)
					throw new InvalidPlanException("Bad value for 'punctattr' for : " + id);
				
				String[] punctAttrs = punctAttrStr.split("[\t| ]+");
				if (punctAttrs.length != 2)
					throw new InvalidPlanException ("Bad value for 'punctattr' for : "+id);
				
					
				pAttr = Variable.findVariable(inputProperties[0], punctAttrs[0]);
				spAttr = Variable.findVariable(inputProperties[1], punctAttrs[1]);

				pSpec = new PunctSpec(e.getAttribute("puncttype"));
				String [] similarityMetric = e.getAttribute("similarity").split("[\t | ]+");
				sSpec = new SimilaritySpec(similarityMetric[0], 
						Integer.valueOf(similarityMetric[1]), Integer.valueOf(similarityMetric[2]));
				pfSpec = new PrefetchSpec(e.getAttribute("prefetch"));
				
				queryString = e.getAttribute("query_string").trim();
				timeAttr =  e.getAttribute("timeattr").trim();
    }
}
