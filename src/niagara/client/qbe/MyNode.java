
/**********************************************************************
  $Id: MyNode.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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
//done up to (No) nest
// have applied the join index method 

// Input a vector of Type Input, each input has same inWhichElement (xml)
// join: let's first assume join is always appied to the leaf node
// join: need to pass the children tree in? or knowing how to traverse
// nest 

import java.util.*;

/**
 *
 *
 */
class MyNode
{
    
    String  name;
    boolean isDtdLeaf; 
    boolean isAttribute;
    boolean isProjected;
    String  predicate;
    int     joinIndex;
    boolean isConstructed;
    Vector  children; // element of Node type
    String globalSymbol;
}
