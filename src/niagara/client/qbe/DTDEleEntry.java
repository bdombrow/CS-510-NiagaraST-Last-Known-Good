
/**********************************************************************
  $Id: DTDEleEntry.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.qbe;
import java.util.*;

///////////////////// 
// DTD element entry
/////////////////////
class DTDEleEntry
{
    public String eleName;
    public Vector eleContents;
    public Vector attrs;

    DTDEleEntry(String ele_name, Vector ele_contents, Vector attributes)
    {
	eleName = ele_name;

	eleContents = new Vector();
	for( int i = 0; i < ele_contents.size(); i++ )
	{
	    String thisContent = (String) (ele_contents.elementAt(i));

	    if( thisContent != "#PCDATA" )
		eleContents.addElement(thisContent);
	}
	attrs = new Vector();
	for( int i = 0; i < attributes.size(); i++ )
	{
	    String thisAttr = (String) (attributes.elementAt(i));
	    attrs.addElement(thisAttr);
	}
    }
}
