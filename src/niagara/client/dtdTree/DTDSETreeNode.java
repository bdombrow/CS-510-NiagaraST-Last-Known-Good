
/**********************************************************************
  $Id: DTDSETreeNode.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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
 * Nodes for the search engine
 *
 */

public class DTDSETreeNode extends DTDTreeNode
{
	// further data
	private String predicate;
	private boolean is_checked;
	
	// only used in the DTD Tree generator.
	DTDTreeNode create(String name, int type)
		{
			return new DTDSETreeNode(name, type);
		}

	public DTDSETreeNode()
		{
			super();
		}

	public DTDSETreeNode(String name, int type)
		{
			super(name, type);
		}
	
	public String getPredicate()
		{
			return predicate;
		}
	
	public void setPredicate(String predicate)
		{
			this.predicate = predicate;
		}

	public boolean isChecked()
		{
			return is_checked;
		}

	public void setCheckedFlag(boolean flag)
		{
			is_checked = flag;
		}

	public String toString()
		{
			String ret = super.toString();
			
			return ret;
		}
}

