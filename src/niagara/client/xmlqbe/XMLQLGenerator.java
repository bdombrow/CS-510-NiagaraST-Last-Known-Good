
/**********************************************************************
  $Id: XMLQLGenerator.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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

import javax.swing.tree.*;
import java.util.*;
import java.net.*;

import niagara.client.dtdTree.*;
import niagara.client.*;


/**
 * this class receives an annotated tree of DefaultMutableTreeNodes and
 * converts it to XMLQL. The user objects of the nodes are of type 
 * DTDXMLQLTreeNode and have the necessary information to generate the query
 *
 */

public class XMLQLGenerator
{
    // the trees of the query
    DefaultMutableTreeNode [] trees = null;
    // the dtds for the corresponding in clauses
    String [] dtds = null;
    
    // Query blocks
    StringBuffer implicitCBlock = new StringBuffer();
    StringBuffer constructBlock = new StringBuffer();
    StringBuffer predicateBlock = new StringBuffer();
    StringBuffer [] inBlocks = null;
    
    StringBuffer joinBlock;
    
    /**
     * Ctor
     * @param t array of the query trees
     * @param dtd array of the dtds for the in-clause
     */
    public XMLQLGenerator(
		DefaultMutableTreeNode [] t, 
		String [] dtd, StringBuffer join)
		{
			trees = t;
			dtds = dtd;
			inBlocks = new StringBuffer[t.length];
			joinBlock = join;
		}
    
    /**
     * Ctor
     * @param t array of the query trees
     * @param dtd array of the dtds for the in-clause
     */
    public XMLQLGenerator(
		DefaultMutableTreeNode t, 
		String dtd)
		{
			trees = new DefaultMutableTreeNode[1];
			dtds = new String[1];
			inBlocks = new StringBuffer[1];
			trees[0] = t;
			dtds[0] = dtd;
		}
    
    /**
     * used for identation
     */
    private String identer(int n)
		{
			StringBuffer sb = new StringBuffer();
			for(int i = 0; i < n; i++){
				sb.append("  ");
			}
			return sb.toString();
		}
    
    /**
     * this processes a single tree
     * @param idx the index of the tree in the trees array
     */
    private void processSingleTree(int idx)
		{
			// Do a depthfirst traversal of the tree 
			// and update the qBuf fields of the user
			// objects in the nodes
	
			StringBuffer sb = null;
	
	// Current node
			DefaultMutableTreeNode n = null;
			DTDXMLQLTreeNode nObj = null;
	
	// Parent of Current node
			DefaultMutableTreeNode nP = null;
			DTDXMLQLTreeNode nPObj = null;
	
			Enumeration e = trees[idx].depthFirstEnumeration();
	
			while(e.hasMoreElements()){
				n = (DefaultMutableTreeNode)(e.nextElement());
				nObj = (DTDXMLQLTreeNode)(n.getUserObject());
				int level = n.getLevel();
	    
				if(!(n.isRoot())){
					nP = (DefaultMutableTreeNode)(n.getParent());
					nPObj = (DTDXMLQLTreeNode)(nP.getUserObject());
				}
	    
				sb = nPObj.getQueryBuffer();
				//if(isLeafElement(n)){
				if(n.isLeaf()){
						
					// see if there are any predicates (only for leaves)
					if(nObj.hasPredicate()){
						//prepend comma
						predicateBlock.append(",\n"+" "+nObj.getPredicate());
					}
		
					if(nObj.getType() == DTDTreeNode.ELEMENT || !isLeafElement(nP)){
						if(nObj.isProjected() || nObj.isJoined() || nObj.hasPredicate()) {
							sb.append("\n" + identer(level) +
									  "<" + nObj.getName() + ">" +
									  nObj.getVariableName() + "</>");
							// select the parent object so that 
							// query generation will continue
							nPObj.select();
						}
					} else {
		    
						String s = sb.toString();
						int gtIdx = s.indexOf(">");
						if(nObj.isProjected() || nObj.isJoined() || nObj.hasPredicate() ) {
							sb.insert(gtIdx, 
									  " " + nObj.getName() + "=" +
									  nObj.getVariableName());
							// select the parent object so that 
							// query generation will continue
							nPObj.select();
						}
						
						if(gtIdx == s.length() - 1){
							if(isLeafElement(nP) || nObj.isProjected()){
								sb.append(nPObj.getVariableName());
								if(nPObj.hasPredicate()){
									predicateBlock.append(",\n"+" "+nPObj.getPredicate());
								}
							}
						}
	
					}
				} else if(!(n.isRoot())){
					// non leaf case
					if(nObj.isProjected()){// handle projection
						if(nObj.getQueryBuffer().toString().equals(">")){
							// simple projection
							sb.append("\n" + identer(level) +
									  "<" + nObj.getName() + ">" +
									  nObj.getVariableName() + "</>");
						} else {
							String s = nObj.getQueryBuffer().toString();
							// if(!(nObj.getQueryBuffer().toString().trim().endsWith("/>"))){
							// 	   s = nObj.getQueryBuffer().toString() + nObj.getVariableName() + "tmp";
							// 							}
							sb.append("\n" + identer(level) +
									  "<" + nObj.getName() + s +
									  "\n" + identer(level));
							if(!isLeafElement(n)){
								sb.append("</> content_as " + nObj.getVariableName());
							} else {
								sb.append("</>");
							}
						}
						// select the parent object so that 
						// query generation will continue
						nPObj.select();
					}
					else if(nObj.isSelected()){// handle selection
						String s = nObj.getQueryBuffer().toString();
//  						if(!(nObj.getQueryBuffer().toString().trim().endsWith("/>"))){
//  							// Insert a variable when only attributes are selected
//  							s = nObj.getQueryBuffer().toString() + nObj.getVariableName() + "tmp";
//  						}
						
						sb.append("\n" + identer(level) +
								  "<" + nObj.getName() + s + 
								  "\n" + identer(level) + "</>");
						// select the parent object so that 
						// query generation will continue
						nPObj.select();
					}						
				} else {
					if(sb.toString().equals(">")){
						nObj.setQueryBuffer( 
							new StringBuffer("\n" + identer(level) + 
											 "<" + nObj.getName() + ">" + 
											 nObj.getVariableName() +
											 identer(level) + "</>\n"));
					} else {
						sb.insert(0, "\n<" + nObj.getName());
						sb.append("\n</>");
						if(nObj.isProjected()){
							sb.append(" content_as " + nObj.getVariableName());
						}
						sb.append("\n");
					}					
					inBlocks[idx] = new StringBuffer(nObj.getQueryBuffer() +
													 "IN \"*\" conform_to \"" +
													 dtds[idx] + "\"");
				}
	    
			}
		}

	private boolean isLeafElement(DefaultMutableTreeNode n){
		// get the nodes children
		Enumeration children = n.children();

		boolean allAttributes = true;
				
		while(children.hasMoreElements()){
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)(children.nextElement());
			DTDXMLQLTreeNode nodeContent= (DTDXMLQLTreeNode)(node.getUserObject());
			allAttributes = allAttributes && nodeContent.isAttribute();
		}
				
		return allAttributes || n.isLeaf();
	}
    
    /**
     * generates the construct block doing
     * preorder traversal of the tree.
     */
    private void generateConstructBlock(int idx)
		{
			// Current node
			DefaultMutableTreeNode n = null;
			DTDXMLQLTreeNode nObj = null;
	
			Enumeration e = trees[idx].preorderEnumeration();
	
			while(e.hasMoreElements()){
				n = (DefaultMutableTreeNode)(e.nextElement());
				nObj = (DTDXMLQLTreeNode)(n.getUserObject());
				// check to see if the element/or
				// attribute is projected and put it
				// in the construct block
				if(nObj.isProjected()){
					constructBlock.append("\n  " + 
										  "<" + nObj.getName() + ">" +
										  nObj.getVariableName() +
										  "</>");
				}	
			}
		}			
    
    
    /**
     * Generates the XMLQL query from the trees
     * @return the query string
     */
    public String generateQuery()
		{
			StringBuffer sb = new StringBuffer("WHERE");
			for(int i = 0; i < trees.length; i++){
				processSingleTree(i);
				generateConstructBlock(i);
			}
	
			// add the pieces together and return them
			for(int i = 0; i < trees.length; i++){
				if(i>0){
					sb.append(",");
				}
				sb.append(inBlocks[i]);
			}
	
			sb.append(predicateBlock);
	
			if ( predicateBlock.toString().indexOf("$") == - 1 ) 
				sb.append("\n");
	
			sb.append(joinBlock);
	
			if(constructBlock.toString().length() > 0){
				sb.append("\nCONSTRUCT");
				int bc = sb.toString().length();
				sb.append(constructBlock);
				if((new StringTokenizer(constructBlock.toString(),"\n")).countTokens() > 1){
					sb.insert(bc, "\n<result>");
					sb.append("\n</>");
				}
			}
	
			return sb.toString();
		}
}
