
/**********************************************************************
  $Id: QueryFrame.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


import java.io.*;
import java.awt.*;
import java.awt.event.*;
import java.applet.*;
import javax.swing.*;
import javax.swing.border.*;


/**
 *  QueryFrame class is a Jframe and acts as a container for
 *  the QueryDialog JDialog fram
 */
class QueryFrame extends JFrame
{

    private String title;
    public QueryDialog queryDialog;

    //////////////
    // constructor
    //////////////
    public QueryFrame(String title, ObjectDes objectDes)
    {
        this.title = title;

        this.setBackground(Color.lightGray);

        // dialog
        this.queryDialog = new QueryDialog(this, "XML Query Dialog", objectDes);
        this.queryDialog.setModal(true);
        this.queryDialog.setBounds(100, 100, 500, 400);
        this.queryDialog.setVisible(true);
    }
}
