
/**********************************************************************
  $Id: debug.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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



package niagara.utils;
public class debug
{
    static final int DEBUG = 1;

    public static void mesg(String s) {
        mesg(s, 1);
    }

    public static void var(Object v) {
        var(v, 1);
    }

    public static void mesg(String s, int level)
    {
	if(( DEBUG & level) != 0 ) System.out.println(s);
    }

    public static void var(Object v, int level)
    {
        if(v==null) {
            if( (DEBUG & level) != 0) System.err.println("obj is null");
        }
	if( (DEBUG & level) != 0 ) System.out.println(v);
    }
}

