
/**********************************************************************
  $Id: JoinColor.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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
import java.awt.*;

class JoinColor{
    static int count=0;
    Color nextC;

    JoinColor()
    {
	nextC = Color.blue; 
    }

    public Color nextColor(){

	count++;

	switch(count%7){
	case 0:
	    nextC = Color.cyan;
	    break;
	case 1:
	    nextC = Color.blue;
	    break;
	case 2:
	    nextC = Color.magenta;
	    break;
	case 3:
	    nextC = Color.orange;
	    break;
	case 4:
	    nextC = Color.yellow;
	    break;
	case 5:
	    nextC = Color.pink;
	    break;
	case 6:
	    nextC = Color.gray;
	    break;
	}

	return nextC;
    }
}
