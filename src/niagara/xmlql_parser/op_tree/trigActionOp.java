
/**********************************************************************
  $Id: trigActionOp.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
  * <code>trigActionOp</code> is the class for trigger Action operator
  * @version 1.0
  * @see op
  */
package niagara.xmlql_parser.op_tree;

import java.util.*;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class trigActionOp extends unryOp {
    private Vector action;
    
    public trigActionOp(Class[] al) {
        super(new String("TrigAction"), al);
    }

    public void setAction(Vector act) {
        action = act;
    }

    public Vector getAction() {
        return action;
    }
}

