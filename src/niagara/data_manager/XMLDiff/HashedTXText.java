
/**********************************************************************
  $Id: HashedTXText.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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


package niagara.data_manager.XMLDiff;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import org.xml.*;
import java.util.*;


public class HashedTXText extends TXText 
{
    public HashedTXText(TXText txt) {
        super(txt.getText());
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof HashedTXText)) return false;
        
        HashedTXText that = (HashedTXText)obj;
        String tthis = getText();
        String tthat = that.getText();

        if(tthis==null) return(tthat==null);
        else return(tthis.equals(tthat));
    }

    public int hashCode() {
        String s = getText();
        return s.hashCode();
    }
}
        
