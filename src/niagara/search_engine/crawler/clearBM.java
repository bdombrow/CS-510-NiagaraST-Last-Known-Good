
/**********************************************************************
  $Id: clearBM.java,v 1.2 2003/07/09 04:59:42 tufte Exp $


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



package niagara.search_engine.crawler;


 /**
  * Routine that periodically cleans the bitmaps.
  *
  *
  */


 public class clearBM implements Runnable {

 private static long interval;

 // default interval is a week's time.

  public clearBM() {
 
  interval  = StorageManager.EXPIRESIN * 1000; 
  }

  public void setInterval(long period) {
 
  interval = period;
  }


  public void run() {

  try {
  Thread.sleep(interval);
  BMStorageManager.clearBitMaps();
  }catch(Exception e) {
    System.out.println("Exception while sleeping" + e);
  }
  }

 }
