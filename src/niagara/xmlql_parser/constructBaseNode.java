/**********************************************************************
  $Id: constructBaseNode.java,v 1.1 2003/12/24 01:19:55 vpapad Exp $


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


/**
*
* base class for representing construct part
*
*/
package niagara.xmlql_parser;

import niagara.optimizer.colombia.Attrs;

public abstract class constructBaseNode {
    // replace the occurences of variables with their corresponding 
    // schema attributes
    abstract public void replaceVar(varTbl vt);
    
    // to print to the standard output
    abstract public void dump(int n);
    
    abstract public Attrs requiredInputAttributes(Attrs attrs);
}
