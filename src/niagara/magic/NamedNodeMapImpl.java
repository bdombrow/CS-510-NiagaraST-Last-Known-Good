
/**********************************************************************
  $Id: NamedNodeMapImpl.java,v 1.1 2003/07/03 19:47:03 tufte Exp $


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

public class NamedNodeMapImpl implements NamedNodeMap {

    Node[] nodeArray;
    int currIdx;

    public NamedNodeMapImpl() {
	nodeArray = new Node[5];
	currIdx = 0;
    }

    public int getLength() {
	return currIdx;
    }

    public Node getNamedItem(String name) {
	for(int i = 0; i<currIdx; i++) {
	    if(nodeArray[i].getNodeName().equals(name))
		return nodeArray[i];
	}
	return null;
    }

    public Node getNamedItemNS(String namespace, String locaName) {
	assert false : "Namespaces not supported in magic code";
	return null;
    }

    public Node item(int index) {
	return nodeArray[index];
    }

    public Node removeNamedItem(String name) {
	assert false : "Updates not supported in magic code";
	return null;
    }

    public Node removeNamedItemNS(String namespace, String localName) {
	assert false : "Namespaces not supported in magic code";
	return null;
    }

    public Node setNamedItem(Node arg) {
	// see if one with this name exists
	for(int i = 0; i<currIdx; i++) {
	    if(nodeArray[i].getNodeName().equals(arg.getNodeName())) {
		Node prevNode = nodeArray[i];
		nodeArray[i] = arg;
		return prevNode;
	    }
	}

	// if not just add it to the end
	checkSpace();
	nodeArray[currIdx] = arg;	    
	currIdx++;
	return null;
    }

    public Node setNamedItemNS(Node arg) {
	assert false : "Namespaces not supported in magic code";
	return null;
    }

    private void checkSpace() {
	if(currIdx < nodeArray.length)
	    return;
	else {
	    assert currIdx == nodeArray.length;
	    Node[] newArray = new Node[currIdx*2];
	    for(int i = 0; i<currIdx; i++)
		newArray[i] = nodeArray[i];
	    nodeArray = newArray;
	}
	    
    }
}






