/**********************************************************************
  $Id: MagicShellElement.java,v 1.2 2003/12/24 02:00:54 vpapad Exp $


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

/**
 * <code>MagicRoot</code> DOM element - but has a pointer to
 * tuple...
 */

public class MagicShellElement extends MagicBaseNode 
    implements Node, Element, NodeList {

    MagicBaseNode[] children;
    int numChildren;
    NamedNodeMapImpl attrs;
    Node nextSibling;
    private String tagName;

    public MagicShellElement(String tagName, MagicBaseNode root) {
	super(root);
	this.tagName = tagName;
	children = new MagicBaseNode[5];
	numChildren = 0;
	attrs = new NamedNodeMapImpl();
	nextSibling = null;
    }

    public void setNextSibling(Node next) {
	nextSibling = next;
    }

    public void setYourNode(Tuple cTuple) {
	for(int i = 0; i<numChildren; i++)
	    children[i].setYourNode(cTuple);
	for(int i = 0; i<attrs.getLength(); i++)
	    ((MagicBaseNode)attrs.item(i)).setYourNode(cTuple);
    }

    public int getLength() {
	return numChildren;
    }

    public Node item(int idx) {
	return children[idx];
    }

    // NODE INTERFACE METHODS: 
    public Node appendChild(Node newChild) {
	assert false : "appendChild not supported - use DNS version";
	throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
			       "appendChild not supported - use DNS version");
    }

    // IF YOU USE THIS FUNCTION, YOU MUST SET THE NEXT SIBLING
    // POINTER!!!!!
    // DNS stands for Do Next Sibling
    public void appendChildDNS(MagicBaseNode newChild) {
	checkChildrenSpace();
	children[numChildren] = newChild;
	numChildren++;
    }

    public Node cloneNode(boolean deep) throws DOMException {
	assert false : "Can not clone MagicShell";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Can not clone MagicShell");
	/*
	Element clone = new MagicShellElement(this.tagName);
	for(int i = 0; i<numChildren; i++)
	    clone.children[i] = children[i];
	for(int i = 0; i<numAttrs; i++)
	    clone.attrs[i] = attrs[i];
	System.err.println("KT: WARNING DANGEROUS CLONE");
	*/
    }

    public NamedNodeMap getAttributes() {
	return attrs;
    }

    public NodeList getChildNodes() throws DOMException {
	return this;
    }

    public Node getFirstChild() throws DOMException {
	if(numChildren == 0)
	    return null;
	else
	    return children[0];
    }

    public Node getLastChild() {
	if(numChildren == 0)
	    return null;
	else 
	    return children[numChildren-1];
    }

    public String getLocalName() {
         return tagName;
    }

    public String getNamespaceURI() throws DOMException {
	assert false : "namespaces not supported in magic content";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
			       "namespaces not supported in magic content");
    }

    public Node getNextSibling() {
	return nextSibling;
    }

    public String getNodeName() {
        return tagName;
    }

    public short getNodeType() {
        return Node.ELEMENT_NODE;
    }

    public String getNodeValue() {
	return null; // correct for element??
    }
    
    public String getPrefix() throws DOMException {
	assert false : "getPrefix unsupported on MagicContent";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "getPrefix unsupported on MagicContent");
    }

    public Node getPreviousSibling() {
	assert false : "getPrevious sibling unsupported on MagicContent";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                           "getPreviousSibling unsupported on MagicContent");
    }

    public boolean hasAttributes() {
	if(attrs == null)
	    return false;
	else
	    return true;
    }

    public boolean hasChildNodes() {
	if(numChildren == 0)
	    return false;
	else
	    return true;
    }

    public boolean isSupported(String feature, String version) {
	return false; // HACK, HACK, HACK
    }

    public void normalize() throws DOMException {
	assert false : "normalize not supported on MagicShell";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "normalize not supported on MagicShell");
    }

    // ELEMENT INTERFACE METHODS
    public String getAttribute(String name) {
        return (attrs.getNamedItem(name)).getNodeValue();
    }

    public Attr getAttributeNode(String name) {
        return (Attr)attrs.getNamedItem(name);
    }

    public Attr getAttributeNodeNS(String namespaceURI, String localName) {
	assert false : "Namespaces not supported in Magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    public String getAttributeNS(String namespaceURI, String localName) {
	assert false : "Namespaces not supported in Magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    public NodeList getElementsByTagName(String name) {
	assert false : "Need node list implementation!";
	return null;
    }

    public NodeList getElementsByTagNameNS(String namespaceURI, 
                                           String localName) {
	assert false : "Namespaces not supported in Magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    public String getTagName() {
        return tagName;
    }

    public boolean hasAttribute(String name) {
	if(attrs.getNamedItem(name) != null)
	    return true;
	else
	    return false;
    }

    public boolean hasAttributeNS(String namespaceURI, String localName) {
	assert false : "Namespaces not supported in Magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    public void removeAttribute(String name)
        throws DOMException {
	assert false : "magic nodes are read only";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "removeAttribte not supported in MagicShell");
    }

    public Attr removeAttributeNode(Attr oldAttr)
        throws DOMException {
	assert false : "magic nodes are read only";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "removeAttribte not supported in MagicShell");
    }

    public void removeAttributeNS(String namespaceURI, String localName)
        throws DOMException {
	assert false : " namespaces not supported in magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "namespaces not supported in magic");
    }

    public void setAttribute(String name, String value)
        throws DOMException {
	assert false : "magic nodes are read only";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "setAttribute not supported in MagicShell");
    }

    public Attr setAttributeNode(Attr newAttr)
        throws DOMException {
        if(attrs == null)
	    attrs = new NamedNodeMapImpl();
	return (Attr)attrs.setNamedItem(newAttr);
    }

    public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
	assert false : " namespaces not supported in magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    public void setAttributeNS(String namespaceURI, String qualifiedName, 
                               String value)
        throws DOMException {
	assert false : " namespaces not supported in magic";
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "Namespaces not supported in Magic.");
    }

    private void checkChildrenSpace() {
	if(numChildren < children.length)
	    return;
	else {
	    assert numChildren == children.length;
	    MagicBaseNode[] newKids = new MagicBaseNode[numChildren*2];
	    for(int i = 0; i<numChildren; i++)
		newKids[i] = children[i];
	    children = newKids;
	}
    }

}
