
/**********************************************************************
  $Id: Const.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.util;

/**
 * constant declaration used in search engine package.
 *
 *
 */
public interface Const {
    //client-server networking properties
    public static final int SERVER_PORT=1710;
    public static final int MAX_CLIENTS=100;

    public static final String RESULT="RESULT";
    public static final String END_RESULT="END_RESULT";
    public static final String COLUMN_DELIMETER="--";
    public static final String QUERY_DELIMETER="|";

    //Column names
    //    public static final String TITLE = "title";
    public static final String URL = "url";
    public static final String TEXT = "text";
    //    public static final String SURL = "surl";

    //server protocol
    public static final String QUERY = "QUERY";
    public static final String SQUERY = "SQUERY";
    public static final String RELOAD = "RELOAD"; 
    public static final String SRELOAD = "SRELOAD"; 
    public static final String INDEX = "INDEX"; 
    public static final String SINDEX = "SINDEX"; 
    public static final String ADDURL = "ADDURL"; 
    public static final String FLUSH = "FLUSH"; 
    public static final String SFLUSH = "SFLUSH"; 
    public static final String QUIT="QUIT";
    public static final String SHUTDOWN="SHUTDOWN";
    public static final String SSHUTDOWN="SSHUTDOWN";

}


