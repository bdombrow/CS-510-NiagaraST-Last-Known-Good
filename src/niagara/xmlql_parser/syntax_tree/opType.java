
/**********************************************************************
  $Id: opType.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * This class declares different operators in XML-QL
 *
 */
package niagara.xmlql_parser.syntax_tree;

public class opType {
     
     public static final int UNDEF = -1;
     
     public static final int STAR = 0;
     public static final int PLUS = 1;
     public static final int QMARK = 2;
     public static final int DOT = 3;
     public static final int BAR = 4;
     public static final int DOLLAR = 5;
     
     public static final int LT = 6;
     public static final int GT = 7;
     public static final int LEQ = 8;
     public static final int GEQ = 9;
     public static final int NEQ = 10;
     public static final int EQ = 11;

     public static final int OR = 14;
     public static final int AND = 15;
     public static final int NOT = 16;

     public static final int ONCE = 17;
     public static final int MULTIPLE = 18;

     public static final int CREATE_TRIG = 19;
     public static final int DELETE_TRIG = 20;
     public static final int XMLQL = 21;

     public static final int CONTAIN = 30;
     public static final int DIRECT_CONTAIN = 31;
     public static final int IS = 32;
};



