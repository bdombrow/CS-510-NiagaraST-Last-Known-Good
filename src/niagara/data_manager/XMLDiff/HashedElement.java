
/**********************************************************************
  $Id: HashedElement.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


//
// XMLDiff Diffing two similiar simple XML file
// Part of Niagra Warm up
//

/**
 * A <code>HashedElement</code> extends <code>TXElement</code>
 * by adding a hashcode to it.  Two HashedElement with same code
 * suggest high possiblility that they are exactly the same
 *
 * @version 0.1
 */
package niagara.data_manager.XMLDiff;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import org.xml.*;
import java.io.*;
import java.util.*;


class HSignature
{
    public int hashcode;
    public int elecount;
    public IntSet samples;

    public HSignature() {
	hashcode = 0;
	elecount = 1;
	samples = new IntSet();
    }

    public void merge(HSignature s) {
	hashcode += s.hashcode;
	elecount += s.elecount;
	samples.merge(s.samples);
    }
    
    public boolean goodMatch(HSignature s) {
	int dc = elecount - s.elecount;
	if(dc < 0) dc = -dc;
	double total = (double)(elecount + s.elecount);
	if(((double)dc)/total > 1.0) return false;
        // System.err.println("HashedElement goodMatch resolved by samples");
	return(samples.goodMatch(s.samples));
    }
}

public class HashedElement extends TXElement
{
    HSignature sig;
    
    public HashedElement(TXElement ele, boolean t) {
        super(ele.getTagName());
        TXElement tele = new TXElement(ele.getTagName());
        Child[] ca = ele.getChildrenArray();
        for(int i=0; i<ca.length; i++) {
            if(ca[i] instanceof TXElement) {
                tele.appendChild(new HashedElement((TXElement)ca[i], true));
            } else
                tele.appendChild(ca[i]);
        }
        sig = computeSig(tele);
        sig.samples.insert(sig.hashcode);
    }
            
    public HashedElement(TXElement ele) {
        super(ele.getTagName());
        sig = computeSig(ele);
        sig.samples.insert(sig.hashcode);
    }

    public Object clone() {
        return new HashedElement(this);
    }
    /**
     * Overwrite equals method, so that 2 Element equal
     * when logically the same
     */

    public boolean equals(Object obj)
    {

	if(!(obj instanceof HashedElement)) return false;
	
	HashedElement that = (HashedElement)obj;
	if(hashCode() != that.hashCode()) return false;
	
	String tag = getTagName();
	if(tag==null) {
	    if(that.getTagName()!=null) 
		return false;
	}
	else if( !tag.equals(that.getTagName()) )
	    return false;
	
	// OK.  If comes here, ID and TagName are same.
	// Next, going down the tree to check if childs are
	// the same.
	
	// build a Hashtable holds my childs, and hash that's
	// child into this one.  Only TXText field and TXElement
	// are used in comparison
	
	Hashtable childHash = new Hashtable();

	if( ! hasChildNodes() ) {
	    if( that.hasChildNodes() ) return false;
	}
	else if( !that.hasChildNodes() ) return false;
	else {
	    NodeList nl = getChildNodes();
	    int size = nl.getLength();
	    for(int i=0; i<size; i++) {
		Node n = nl.item(i);
		if(n instanceof TXText) {
		    HashedTXText t = new HashedTXText((TXText)n);
		    Integer count = (Integer)childHash.get(t);
		    if(count!=null) {
		      childHash.put(t, new Integer(count.intValue()+1));
		    }
		    else childHash.put(t, new Integer(1));
		}
		else if(n instanceof HashedElement) {
		    HashedElement child = (HashedElement)n;
		    Integer count = (Integer)childHash.get(child);
		    if(count!=null) {
			childHash.put(child, new Integer(count.intValue()+1));
		    }
		    else childHash.put(child, new Integer(1));
		}
	    }
		
	    NodeList thatnl = that.getChildNodes();
	    int thatsize = thatnl.getLength();
	    for(int j=0; j<thatsize; j++) {
	      Node nn = nl.item(j);
	      if(nn instanceof TXText) {
		HashedTXText ns = new HashedTXText((TXText)nn);
		Integer count = (Integer)childHash.get(ns);
		if(count!=null) {
		  childHash.put(ns, new Integer(count.intValue()-1));
		}
		else childHash.put(ns, new Integer(-1));
	      }
	      if(nn instanceof TXElement) {
		HashedElement tc = (HashedElement)nn;
		Integer count = (Integer)childHash.get(tc);
		if(count!=null) {
		  childHash.put(tc, new Integer(count.intValue()-1));
		}
		else childHash.put(tc, new Integer(-1));
	      }
	    }

	    Enumeration enum = childHash.elements();
	    while(enum.hasMoreElements()) {
	      Integer tint = (Integer)enum.nextElement();
	      if(tint.intValue()!=0) return false;
	    }
	}
        return true;
    }

    /**
     * Overwrite the default hashCode method, so that
     * logical equivalent TXElement will have same hashCode
     */
    public int hashCode()
    {
	return sig.hashcode;
    }

    public HSignature computeSig(TXElement ele) 
    {
	HSignature ret = new HSignature();
	String tag = getTagName();
	if(tag!=null) {
	    ret.hashcode += tag.hashCode();
	} 
        
	if(ele.hasChildNodes()) {
            Child[] ca = ele.getChildrenArray();
            String tt = new String();
            for(int i=0; i<ca.length; i++) {
                Child tmp = ca[i];
                if(tmp instanceof TXText) {
                    String t = tmp.getText();
                    String t2 = t.trim();
                    if(t2!=null && !t2.equals("")) {
                        tt += t2;
                    }
                }
                else {
                    ret.merge(((HashedElement)tmp).sig);
                    this.appendChild((Node)tmp.clone());
                }
            }

            if(!tt.equals("")) {
                ret.hashcode += tt.hashCode();
                this.appendChild(new TXText(tt));
            }

        }
        return ret;
    }	 

    public boolean goodMatch(HashedElement other)
    {
        return(sig.goodMatch(other.sig));
    }
}










