
/**********************************************************************
  $Id: DTD.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.dtdTree;

import javax.swing.*;
import javax.swing.tree.*;
import java.net.*;
import java.io.*;
import java.util.*;
import com.microstar.xml.*;
import niagara.client.*;

/**
 * Collection of utilites
 *
 */

public class DTD
{
	private static boolean debug = false;

	// The maximal number of recursive elements
	// (this is used when the dtd is recursive)
	private static final int MAX_RECURSION = 10;

	/**
	 * figure out the root of the dtd
	 */
	private static String getDTDRoot(String dtdBuf)
		{
			// find out the root of the dtd
			String l = null;
			BufferedReader reader = null;
			StringTokenizer lb = null;
			try{
				reader = new BufferedReader(new StringReader(dtdBuf));
				
				boolean done = false; 
				while(!done){
					l = reader.readLine();
					l = l.trim();
					if(l.startsWith("<!ELEMENT")) done = true;
				}
				
				if(l.endsWith("<!ELEMENT")){
					l = reader.readLine();
					l = l.trim();
					lb = new StringTokenizer(l);
				} else {
					lb = new StringTokenizer(l);
					lb.nextToken(); // advance <!ELEMENT
				}
			}
			catch(IOException e){
				e.printStackTrace();
			}
			
			// This is the root of the DTD
			String root = lb.nextToken();
			return root;
		}
	
	
	/**
	 * Extracts the dtd string from the specified URL
	 * @param url the URl
	 */
	private static String getDTDString(URL url)
		{
			BufferedReader reader = null;
			String l = null;
			// read in the whole dtd file into a string
			StringBuffer dtdBuf = new StringBuffer();
			try{
				reader = new BufferedReader(new InputStreamReader(url.openStream()));
				
				l = reader.readLine();
				while(l != null){
					dtdBuf.append(l+"\n");
					l = reader.readLine();
				}
			}
			catch(IOException e){
				e.printStackTrace();
			}				
			
			return dtdBuf.toString();
		}
	

	/**
	 * This creates a tree with nodes of a specified 
	 * class for display with a JTree
	 * @param url url of dtd
	 * @param className the class name to use for each node (for now DTDSETreeNode)
	 * @return a MutableTreeNode
	 */
	private static DefaultMutableTreeNode generateTree(URL url, String className)
		{
			return generateTree(getDTDString(url), className);
		}
    /**
	 * This creates a tree with nodes of a specified 
	 * class for display with a JTree
	 * @param dtdBuf the string containing the dtd
	 * @param className the class name to use for each node (for now DTDSETreeNode)
	 * @return a MutableTreeNode
	 */
	private static DefaultMutableTreeNode generateTree(String dtdBuf, String className)
		{
			DTDTreeNode nodeClass = null;
			try{
				Class c = Class.forName("niagara.client.dtdTree."+className);
				nodeClass = (DTDTreeNode)(c.newInstance());
			}catch(ClassNotFoundException e){
				System.err.println("Specify an existing class please");
			}catch(IllegalAccessException e){
				System.err.println("Should not happen");
			}catch(InstantiationException e){
				System.err.println("Should not happen");
			}

			
			// get the root of the dtd
			String root = getDTDRoot(dtdBuf);
			
			// The string to feed the parser with
			String s = "<?xml version=\"1.0\"?><!DOCTYPE " + root + " [\n"+dtdBuf+"\n]>";
			
			// feed the string to the parser, call parse and catch the exception
			Reader sr = new StringReader(s);
			XmlParser parser = new XmlParser();
			
			try{
				parser.parse(null, null, sr);
			}
			catch(Exception e){
				// do nothing
			}

			DefaultMutableTreeNode n = constructTree(root, parser, nodeClass);

			return n;
		}

	private static void fillNode(DTDXMLQLTreeNode n)
		{
			try{
				String s = null;
				BufferedReader br = new BufferedReader(
					new InputStreamReader(System.in));
				
				s = br.readLine();
				if(s.equals("q")) {
					debug = false;
					return;
				}
				
				if(s.length() > 1){
					n.setPredicate(s);
				}

				s = br.readLine();
				if(s.length() > 1){
					n.setVariableName(s);
				}
			
				s = br.readLine();
				if(s.length() > 0){
					n.project();
				}
			}
			catch(IOException w){
			}
			
		}
	


	private static DefaultMutableTreeNode constructTree(
		String node, XmlParser parser, DTDTreeNode nodeClass)
		{
			return constructTree(node, parser, nodeClass, 1);
		}
	
	// dtd tree construction with recursion detection
	private static DefaultMutableTreeNode constructTree(
		String node, XmlParser parser, DTDTreeNode nodeClass, int recLevel)
		{
			// create the node
			DTDTreeNode aDTDTreeNode = nodeClass.create(node, DTDTreeNode.ELEMENT);
			if(debug){
				fillNode((DTDXMLQLTreeNode)aDTDTreeNode);
			}
			DefaultMutableTreeNode treeNode =
				new DefaultMutableTreeNode(aDTDTreeNode);

			// get the content model and its type
			String model = parser.getElementContentModel(node);
			
			// trim model to do away with the encoding information
			model = model.substring(model.indexOf('('), model.length());

			int modelType = parser.getElementContentType(node);
			
			if(recLevel > MAX_RECURSION){
				return treeNode;
			}

			if(modelType != XmlParser.CONTENT_ELEMENTS &&
			   modelType != XmlParser.CONTENT_MIXED){
				return treeNode;
			} else {
				// parse the model
				StringTokenizer st = new StringTokenizer(model,"()*+?,| \t\n");
				
				// and create the children
				List childNodes = new ArrayList();

				while(st.hasMoreTokens()){
					String s = st.nextToken();
					if(!s.equals("#PCDATA")){
						childNodes.add(s);
					}
				}
				
				Iterator it = childNodes.iterator();
				
				while(it.hasNext()){
					String name = (String)(it.next());
					treeNode.add(constructTree(name, parser, nodeClass, recLevel+1));
				}
				
				// add the attributes to the dtd
				Enumeration attrs = parser.declaredAttributes(node);
				
				if(attrs != null) while(attrs.hasMoreElements()){
					String name = (String)(attrs.nextElement());
					int attrType = parser.getAttributeType(node, name);
					if(attrType != XmlParser.ATTRIBUTE_ID){
						DTDTreeNode dtdTreeNode = nodeClass.create(name, DTDTreeNode.ATTRIBUTE);
						if(debug){
							fillNode((DTDXMLQLTreeNode)dtdTreeNode);
						}
						treeNode.insert(new DefaultMutableTreeNode(dtdTreeNode),0);
					}
				}
			}
			return treeNode;
		}

	/**
	 * call this to generate a tree for the search engine
	 * @param url the url to read from
	 */
	public static DefaultMutableTreeNode generateSETree(URL url)
		{
			return DTD.generateTree(url, "DTDSETreeNode");
		}

	/**
	 * generate the tree from the string containing the dtd
	 * @param dtdBuf string with dtd
	 */
	public static DefaultMutableTreeNode generateSETree(String dtdBuf)
		{
			return DTD.generateTree(dtdBuf, "DTDSETreeNode");
		}

	/**
	 * call this to generate a tree for the search engine
	 * @param url the url to read from
	 */
	public static DefaultMutableTreeNode generateXMLQLTree(URL url)
		{
			return DTD.generateTree(url, "DTDXMLQLTreeNode");
		}

	/**
	 * generate the tree from the string containing the dtd
	 * @param dtdBuf string with dtd
	 */
	public static DefaultMutableTreeNode generateXMLQLTree(String dtdBuf)
		{
			return DTD.generateTree(dtdBuf, "DTDXMLQLTreeNode");
		}

    ///////////////////////////
	// Non Gui Testing stuff //
	///////////////////////////
	public static void insertPredicate(
		DefaultMutableTreeNode root,
		String nodeName,
		String predicate)
		{
			Enumeration bf = root.breadthFirstEnumeration();
			
			while(bf.hasMoreElements()){
				DefaultMutableTreeNode n =
					(DefaultMutableTreeNode)(bf.nextElement());
				
				DTDSETreeNode seNode = (DTDSETreeNode)(n.getUserObject());
				
				if(seNode.getName().equals(nodeName)){
					seNode.setPredicate(predicate);
					break;
				}
			}
		}

	// debug main
	public static void main(String argv[]) throws MalformedURLException
		{
			DefaultMutableTreeNode 
 				n = DTD.generateXMLQLTree(
					new URL("file:SigmodRecord.dtd"));
			//  DefaultMutableTreeNode 
//   				n = DTD.generateXMLQLTree(
//  					new URL("http://www.acm.org/sigs/sigmod/record/xml/Record/DTD/SigmodRecord.dtd"));
			
			new JTreeShowThread(n);
		}
}



