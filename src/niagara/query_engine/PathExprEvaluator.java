
/**********************************************************************
  $Id: PathExprEvaluator.java,v 1.7 2002/03/31 04:48:15 vpapad Exp $


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


package niagara.query_engine;

import org.w3c.dom.*;
import java.io.*;
import java.lang.*;
import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.xmlql_parser.op_tree.*;

/**
 *  This class provides functions to get all reachable nodes 
 *  Given an XML Element node and a regular expression.  The
 *  regular expression is in the form of a regExpOpNode.  For
 *  example, the path expression '<CODE>book.(author|editor).name</CODE>'
 *  is represented as:
 *  <pre>
 *               .
 *              / \
 *             .   name
 *            / \
 *         book  |
 *              / \
 *         author editor
 *
 *  </pre>
 *        
 *
 */

public class PathExprEvaluator 
{
            
    /**
     *  Static function used to get all reachable nodes given a path 
     *  expression and an element node to start with
     *
     *  @param inNode the input xml element
     *  @param regExp the regular expression to apply
     *  @return the vector of nodes reachable from inNode by applying regExp
     */
    public static Vector getReachableNodes(Object inNode, regExp rExp)
    {
        // This cannot be true.
        if (inNode==null) System.err.println("Null inNode!!!!");
		
		Vector nodesReached = new Vector();
		int i = 0, j = 0;
		
		if (rExp == null) {
			
			// Incoming regular expression is null - just return the inNode
			//
			nodesReached.add(inNode);
			
			return nodesReached;
		}
		
		// If this is a leaf node, just get all child nodes mathching data name and return
		//
		if (rExp instanceof regExpDataNode) {
		    
			regExpDataNode re = (regExpDataNode)rExp;
			data regExpData = re.getData();
			Object o  = regExpData.getValue();
			String val = null;
			
			if (o instanceof String)
				val = (String)o;
			else{
                System.err.println("Error: Leaf is " + o);
                ((schemaAttribute)o).dump();
				System.err.println("ERROR: leaf of path expression is not a string");
				return nodesReached;
			}
			
			
			// made this a 'switch' in case we add diff type later
			// for now, all types at this point better be strings
			// 
			switch(regExpData.getType()) {
			case dataType.STRING: 
				
				Class inNodeClass = inNode.getClass();
				// if this is not the doc root, get children with name val
				//
				if (inNode instanceof Element) {
                                    // Get all child elements 
                                    Element element = (Element)inNode;
                                 
                                    Node child = element.getFirstChild();
                                    while (child != null) {
                                        if (val.equals(child.getNodeName()))
                                            nodesReached.add(child);
                                        child = child.getNextSibling();
                                    }
                                    
                                    // Get the attributes of the element
                                    NamedNodeMap attrs = element.getAttributes();

                                    // Add the reachable attribute to the vector
                                    Node attr_node = attrs.getNamedItem(val);
                                    if (attr_node != null) {
                                        nodesReached.add(attrs.getNamedItem(val));
                                    }
                                    
                                    return nodesReached;		    
				}
				
				// Else if it is the root of the document, get the doc
				// element if its name == val
				//
				else if (inNode instanceof Document) {
					Element docroot = (Element)((Document)inNode).getDocumentElement();
					if (docroot != null) {
						if (val.equals(docroot.getNodeName()))
							nodesReached.add(docroot);
					}
					return nodesReached;
				}
				else if (inNode instanceof Attr) {
					// If the inNode is an attribute then the path should 
					// definitely be null
					System.out.println("PRED_EV: Bad Attribute Encountered");
				}
				
			case dataType.ATTR:
				System.err.println("Attr type data node in PathExpr");
				break;
				
			default:
				System.err.println("invalid type for dataRegExpr node");
				System.exit(1);
			}
		}
		
		
		// If this is an op node, make the appropriate recursive calls
		//
		else if (rExp instanceof regExpOpNode) {
			
			regExpOpNode re = (regExpOpNode)rExp;
			Vector leftNodes = null;
			Vector rightNodes = null;
			Vector[] all = null;
			int size;
			
			switch(re.getOperator()) {
				
			case opType.BAR:
				
				// Make recursive calls on left and right nodes
				//
				leftNodes = getReachableNodes(inNode, re.getLeftChild());
				rightNodes = getReachableNodes(inNode, re.getRightChild());
				
				// Add the smaller result vector to the larger one
				// and return the result
				//
				if (leftNodes.size() < rightNodes.size()) {
					for (i=0; i<leftNodes.size(); i++)
						rightNodes.add(leftNodes.get(i));
					return rightNodes;
				}
				for (i=0; i<rightNodes.size(); i++)
					leftNodes.add(rightNodes.get(i));
				return leftNodes;
				
			case opType.DOT:
				
				// Get the nodes reachable on the left side of the dot
				//
				leftNodes = getReachableNodes(inNode, re.getLeftChild());
				
				// for each reachable left node, get all reachable right nodes
				//
				int lsize = leftNodes.size();
				for (i = 0; i < lsize; i++) {
					
				    rightNodes = getReachableNodes(leftNodes.get(i), 
								   re.getRightChild());
					
					// Add each reachable right node to the return nodeList
					//
					for (j=0; j< rightNodes.size(); j++)
						nodesReached.add(rightNodes.get(j));
				}
				return nodesReached;
				
				
			case opType.DOLLAR:
				
				// If it is the root, insert the document element to reachable
				//
				if (inNode instanceof Document) {
					nodesReached.add( ((Document)inNode).getDocumentElement() );
				}
				
				// Otherwise put all child elements in reachable
				//
				else if (inNode instanceof Element) {
					
					// Get all child elements
					//
					NodeList nl = ((Element)inNode).getChildNodes();
					size = nl.getLength();
					
					// add each item from node list to the return vector, return it
					//		
					for (i = 0; i < size; i++) {
					    Node n = nl.item(i);
					    if (n instanceof Element)
						nodesReached.add(n);
					}
				}
				
				return nodesReached;
				
				
			case opType.QMARK:
				
				// Get the nodes reachable on the left side of the dot
				//
				leftNodes = getReachableNodes(inNode, re.getLeftChild());
				
				// Append the inNode to this set and return it
				//
				if (inNode instanceof Document)
					leftNodes.add(((Document)inNode).getDocumentElement());
				else
					leftNodes.add(inNode);
				return leftNodes;		
				
			case opType.STAR:
				
				//int size;		
				//Vector [] all;
				
				nodesReached.add(inNode);
				leftNodes = getReachableNodes(inNode, re.getLeftChild());
				while(leftNodes.size() != 0)
				{
					size = leftNodes.size();			
					for (i=0; i<size; i++)
						nodesReached.add(leftNodes.get(i));
					
					all = new Vector[size];			
					for (i=0; i<size; i++)
						all[i] = getReachableNodes(leftNodes.get(i), re.getLeftChild());
					
					leftNodes = new Vector();
					for (i=0; i<size; i++)
						for (j = 0; j < all[i].size(); ++j)
							leftNodes.add(all[i].get(j));
				}		
				
				return nodesReached;
				
			case opType.PLUS:
				
				//Xint size;		
				//Vector [] all;
				
				leftNodes = getReachableNodes(inNode, re.getLeftChild());
				while(leftNodes.size() != 0)
				{
					size = leftNodes.size();			
					for (i=0; i<size; i++)
						nodesReached.add(leftNodes.get(i));
					
					all = new Vector[size];			
					for (i=0; i<size; i++)
						all[i] = getReachableNodes(leftNodes.get(i), re.getLeftChild());
					
					leftNodes = new Vector();
					for (i=0; i<size; i++)
						for (j = 0; j < all[i].size(); ++j)
							leftNodes.add(all[i].get(j));
				}		
				
				return nodesReached;
				
			default:
				System.err.println("Invalid op type in path exp evaluator: ");
				System.exit(1);
			}
		}
		System.err.println("Invalid type for regular expression");
		return nodesReached;
    }
}




