
/**********************************************************************
  $Id: Util.java,v 1.2 2002/08/17 16:50:05 tufte Exp $


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


package niagara.search_engine.indexmgr;

public class Util {

  public static final int toInt (byte[] data, int offset) {
    int ret = ((int)(data[offset]) << 24) & 0xFF000000;
    ret |= ((int)(data[offset+1]) << 16) & 0x00FF0000;
    ret |= ((int)(data[offset+2]) << 8) & 0x0000FF00;
    ret |= ((int)(data[offset+3])) & 0x000000FF;
    return ret;
  }
  public static final void writeInt(int val, byte[] b, int offset) {
    b[offset] = (byte)(val >> 24);
    b[offset+1] = (byte)((val & 0x00FF0000) >> 16);
    b[offset+2] = (byte)((val & 0x0000FF00) >> 8);
    b[offset+3] = (byte)(val & 0x000000FF);
  }
  public static final long toLong (byte[] data, int offset) {
    int part1 = toInt(data, offset);
    int part2 = toInt(data, offset+4);
    return ((long)part1<<32) | part2;
  }
  public static final void writeLong(long val, byte[] b, int offset) {
    writeInt ((int)(val>>32), b, offset);
    writeInt ((int)val, b, offset+4);
  }
  public static final long makeLong(int x, int y) {
    return (long)x << 32 | y;
  }
  public static final void writeShort(short val, byte[] b, int offset) {
    b[offset] = (byte)(val >> 8);
    b[offset+1] = (byte)(val & 0x00FF);
  }
  public static final short toShort(byte[] b, int offset) {
    short ret = (short)((b[offset] << 8) & 0xFF00);
    ret |= b[offset+1] & 0x00FF;
    return ret;
  }

  public static final int max(int a, int b) {
    if (a >= b) return a;
    return b;
  }

  public static final int min(int a, int b) {
    if (a >= b) return b;
    return a;
  }
}
