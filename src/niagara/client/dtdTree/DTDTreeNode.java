
/**********************************************************************
  $Id: DTDTreeNode.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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

/**
 * Generic node of a dtd hierarchy
 *
 */

public abstract class DTDTreeNode
{
	public static final int ELEMENT = 0;
	public static final int ATTRIBUTE = 1;
	
	// the name of the node (element name| attribute name)
	private String name = null;
	private int type = -1;

	// used to create a node of the same class
	DTDTreeNode create(String name, int type)
		{
			return null;
		}
	
	public DTDTreeNode()
		{}

	public DTDTreeNode(String name, int type)
		{
			this.name = name;
			this.type = type;
		}

	public String getName()
		{
			return name;
		}
	
	public int getType()
		{
			return type;
		}

	public boolean isElement()
		{
			return (type == ELEMENT);
		}

	public boolean isAttribute()
		{
			return (type == ATTRIBUTE);
		}

	public String toString()
		{
			return name;
		}
}


