/**********************************************************************
  $Id: MagicBaseNode.java,v 1.2 2003/12/24 02:00:54 vpapad Exp $


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

package niagara.magic;

import org.w3c.dom.*;
import niagara.utils.Tuple;
import niagara.utils.PEException;

/**
 * <code>MagicRoot</code> DOM element - but has a pointer to
 * tuple...
 */

public abstract class MagicBaseNode implements Node { 

    protected MagicBaseNode root;
    private Document doc; // MAJOR HACK !! avoids writing a TextImpl

    protected MagicBaseNode(MagicBaseNode root) {
	this.root = root;
    }
    
    public void youAreTheRoot(Document doc) {
	this.root = this;
	this.doc = doc;
    }

    protected Document getDoc() {
	return doc;
    }

    public void setTuple(Tuple cTuple) {
	root.setYourNode(cTuple);
    }

    public abstract void setYourNode(Tuple cTuple);

    /*
    public StreamTupleElement getCurrentTuple() {
	return currentTuple;
    }
    */

    public void setNextSibling(Node next) throws DOMException {
	assert false : "Calling on wrong class - not supported";
	throw new PEException("Calling on wrong class - not supported");
    }

    public void appendChildDNS(MagicBaseNode newChild) {
	// assert doesn't work here
	assert false : "test0";
	throw new PEException("Calling on wrong class - not supported");
    }

    // NODE interface fcns
    public Document getOwnerDocument() throws DOMException {
	return root.getDoc(); 
    }

    // NODE INTERFACE METHODS: 

    public Node appendChild(Node newChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

    public abstract Node cloneNode(boolean deep);
    public abstract NamedNodeMap getAttributes();
    public abstract NodeList getChildNodes();
    public abstract Node getFirstChild();
    public abstract Node getLastChild();

    public abstract String getLocalName();
    public abstract String getNamespaceURI();
    public abstract Node getNextSibling();

    public abstract String getNodeName();
    public abstract short getNodeType();
    public abstract String getNodeValue() throws DOMException;

    public Node getParentNode() {
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "getParentNode unsupported on MagicNode");
    }
    
    public abstract String getPrefix();
    public abstract Node getPreviousSibling();
    public abstract boolean hasAttributes();
    public abstract boolean hasChildNodes();

    public Node insertBefore(Node newChild, Node refChild)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

    public boolean isSupported(String feature, String version) {
	return false; // HACK! (stolen from Vassilis...)
    }

    public abstract void normalize();

    public Node removeChild(Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

    public Node replaceChild(Node newChild, Node oldChild) 
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

    public void setNodeValue(String nodeValue) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

    public void setPrefix(String prefix) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "Magic objects are read-only.");
    }

}
