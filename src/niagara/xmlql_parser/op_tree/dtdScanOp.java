/**********************************************************************
  $Id: dtdScanOp.java,v 1.7 2003/03/07 23:36:43 vpapad Exp $


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


package niagara.xmlql_parser.op_tree;

import java.util.*;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.*;
import niagara.optimizer.colombia.*;
import niagara.optimizer.rules.Initializable;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This class represent dtd Scan operator that fetches the data sources,
 * parses it and then returns the DOM tree to the operator above it.
 *
 */
public class dtdScanOp extends NullaryOp implements Initializable {
   /** the attribute we create */
   private Attribute variable;
   private Vector docs;// Vector of urls to scan.
   private String type;// dtd name (e.g book.dtd)
   private Vector docsCpy; //trigger. the copy of the docs

    public dtdScanOp() {}
    
    public dtdScanOp(Attribute variable, Vector docs, String type, Vector docsCpy) {
        this.variable = variable;
        this.docs = docs;
        this.type = type;
        this.docsCpy = docsCpy;
    }
    
    public dtdScanOp(dtdScanOp op) {
        this(op.variable, op.docs, op.type, op.docsCpy);
    }
    
    public Op copy() {
        return new dtdScanOp(this);
    }
    
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty lp = null;

        float card = 0;
        boolean local = true;

        // Cardinality is the sum of document cardinalities,
        // locality is the conjunction of document localities
        for (int i = 0; i < docs.size(); i++) {
            lp = catalog.getLogProp((String) docs.get(i));
            card += lp.getCardinality();
            local |= lp.isLocal();
        }
        
        return new LogicalProperty(
            card,
            new Attrs(variable),
            local);
    }

    /** Initialize the dtdscan from a resource operator */
    public void initFrom(LogicalOp op) {
        ResourceOp rop = (ResourceOp) op;
        docs = new Vector();
        docs.addAll(rop.getCatalog().getURL(rop.getURN()));
        variable = rop.getVariable();
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

    public Attribute getVariable() {
        return variable;
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

    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        for (int i = 0; i < docs.size(); i++)
            sb.append("<url value='").append(docs.elementAt(i)).append("'/>");
        sb.append("</dtdscan>");
    }
    
    public boolean isSourceOp() {
	return true;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof dtdScanOp))
            return false;
        if (obj.getClass() != dtdScanOp.class)
            return obj.equals(this);
        dtdScanOp other = (dtdScanOp) obj;
        if ((type == null) != (other.type == null))  return false;
        if (type != null && !type.equals(other.type)) return false;
        return variable.equals(other.variable) && docs.equals(other.docs);
    }

    public int hashCode() {
        int hashCode = 0;
        if (variable != null)
            hashCode ^= variable.hashCode();
        if (docs != null)
            hashCode ^= docs.hashCode();
        if (type != null)
            hashCode ^= type.hashCode();
        return hashCode;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");

        // The node's children contain URLs
        Vector urls = new Vector();
        NodeList children = ((Element) e).getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            urls.addElement(((Element) child).getAttribute("value"));
        }

        setDocs(urls);
        variable = new Variable(id, NodeDomain.getDOMNode());
    }
}
