
/**********************************************************************
  $Id: JTreeShowThread.java,v 1.2 2003/07/08 02:10:37 tufte Exp $


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


package niagara.client;

import javax.swing.JPanel;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.JScrollPane;
import javax.swing.JFrame;
import java.awt.*;


// lgalanis

public class JTreeShowThread implements Runnable
{
	DefaultMutableTreeNode node;
	
	public JTreeShowThread(DefaultMutableTreeNode n)
		{
			node = n;
			(new Thread(this)).start();
		}

	public Component create()
		{
			final JTree tree = new JTree(node);
			JScrollPane treeView = new JScrollPane(tree);
			JPanel pane = new JPanel();
			pane.setLayout(new GridLayout(0, 1));
			
			pane.add(treeView);
			return pane;
		}

	public void run()
		{
			//Create the top-level container and add contents to it.
			JFrame frame = new JFrame("");
			Component contents = create();
			frame.getContentPane().add(contents, BorderLayout.CENTER);
			
			frame.pack();
			frame.setVisible(true);
		}
}
