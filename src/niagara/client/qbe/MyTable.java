
/**********************************************************************
  $Id: MyTable.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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


/**
 *
 *
 */
class MyTable
{

    static final int max=100;
    int []mapping;
    //    int index;

    /**
     *
     *
     */
    MyTable()
    {
	mapping= new int[max];
	for(int i=0; i< max; i++){
	    mapping[i]=-1;
	}
    }


    /**
     *
     *
     */
    int lookUp(int key){return mapping[key]; }


    /**
     *
     *
     */
    void set(int key, int value){ mapping[key]=value; }
}
