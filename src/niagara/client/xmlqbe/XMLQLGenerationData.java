
/**********************************************************************
  $Id: XMLQLGenerationData.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.xmlqbe;

import java.util.Vector;
import javax.swing.tree.*;

public class XMLQLGenerationData {
    private Vector treeNodes;
    private Vector dtdURLs;

    XMLQLGenerationData() {
	treeNodes = new Vector();
	dtdURLs = new Vector();
    }

    public void addTree(DefaultMutableTreeNode node) {
	treeNodes.addElement(node);
    }

    public void addURL(String url) {
	dtdURLs.addElement(url);
    }

    public boolean containsURL(String url) {
	return dtdURLs.contains(url);
    }

    public boolean removeTree(DefaultMutableTreeNode node) {
	return treeNodes.removeElement(node);
    }

    public boolean removeURL(String url) {
	return dtdURLs.removeElement(url);
    }

    public DefaultMutableTreeNode[] getTreeArray() {
	if ( treeNodes.size() == 0 ) return null;

	DefaultMutableTreeNode[] trees = new DefaultMutableTreeNode[treeNodes.size()];

	for ( int i = 0 ; i < treeNodes.size() ; i++ )
	    trees[i] = (DefaultMutableTreeNode)treeNodes.elementAt(i);

	return trees;
    }

    public String[] getURLArray() {
	if ( dtdURLs.size() == 0 ) return null;

	String[] urls = new String[dtdURLs.size()];

	for ( int i = 0 ; i < dtdURLs.size() ; i++ )
	    urls[i] = (String)dtdURLs.elementAt(i);

	return urls;
    }
}
