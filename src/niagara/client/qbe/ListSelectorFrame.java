
/**********************************************************************
  $Id: ListSelectorFrame.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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
import java.util.Vector;
import javax.swing.*;
import javax.swing.border.*;
//import java.swing.event.*;
import java.awt.*;
import java.awt.event.*;

public class ListSelectorFrame extends JDialog 
    implements ActionListener, WindowListener, MouseListener
{
    
    // Not thread safe
    //
    private static Object selectedObject;

    private static Font labelFont = new Font("SansSerif", Font.BOLD, 12);
    private static Font listFont = new Font("SansSerif", Font.PLAIN, 12);
    private static Font buttonFont = new Font("SansSerif", Font.PLAIN, 10);

    private JList dtdlist;
    private JLabel mes;
    private JButton okButton;
    private JButton cancelButton;
    private JScrollPane listScrollPane;
    
    // Constructor
    //
    ListSelectorFrame(Vector vec, String message, String title)
    {
	
	// Set up this window
	//
	setModal(true);
	setBackground(Color.white);
	setSize(600,400);
	setTitle(title);
	//setDefaultCloseOperation();
	
	// Init params
	//
	this.dtdlist = new JList(vec);

	// Add a combo box for the active queries
	//
	dtdlist.setBackground(Color.white);
	dtdlist.setFont(listFont);
	dtdlist.setBounds( 20   /* X      */,
			   60   /* Y      */,
			   1000 /* Width  */,
			   5000 /* Height */);
	dtdlist.setSelectedIndex(0);
	selectedObject = dtdlist.getSelectedValue();
	dtdlist.setVisible(true);
	dtdlist.addMouseListener(this);

	// Create a scroll pane that contains the list
	//
	listScrollPane = new JScrollPane(dtdlist,
					 JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
					 JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
	listScrollPane.setBorder(new EtchedBorder());
	listScrollPane.setBounds( 20   /* X      */,
				  60   /* Y      */,
				  500  /* Width  */,
				  200  /* Height */);
	listScrollPane.setVisible(true);
	
	// Create the User label
	//
	mes = new JLabel(message);
	mes.setForeground(Color.black);
	mes.setFont(labelFont);
	mes.setBounds( 20   /* X      */,
		       20   /* Y      */,
		       400  /* Width  */,
		       40   /* Height */);

	
	okButton = new JButton("Ok");
	okButton.setFont(buttonFont);
	okButton.addActionListener(this);
	okButton.setBounds( 140   /* X      */,
			    320   /* Y      */,
			    100  /* Width  */,
			    25   /* Height */);


	cancelButton = new JButton("Cancel");
	cancelButton.setFont(buttonFont);
	cancelButton.addActionListener(this);
	cancelButton.setBounds( 300   /* X      */,
				320   /* Y      */,
				100   /* Width  */,
				25    /* Height */);
	


	// Create the panel to contain the components
	//
	JPanel dtdPanel = new JPanel();
	dtdPanel.setLayout(null);
	dtdPanel.add(mes);
	dtdPanel.add(listScrollPane);
	dtdPanel.add(okButton);
	dtdPanel.add(cancelButton);
	dtdPanel.setVisible(true);
	dtdPanel.setBackground(Color.white);
	

	// Add component filled panel to the Dialog frame
	//
	this.getContentPane().add(dtdPanel);
	show();
    }
    
    /**
     *
     *
     */
    public Object getSelectedItem()
    {
	return selectedObject;
    }
    

    public void actionPerformed(ActionEvent e) 
    {  
	Object source = e.getSource();
	
	// If acton comes from the combo box
	//
	//if(source instanceof JList){
		
	    // Do something with the list
	    //
	    //selectedObject = ((JList)source).getSelectedItem();
	//   return;
	//}

	// Return the selected object or null
	//
	if(source instanceof JButton){
	    
	    JButton btn = (JButton)source;

	    if(btn != okButton) selectedObject = null;
	    this.setVisible(false);
	    this.dispose();
	}		
    }

    //
    // Window Listener stubs
    //
    public void windowClosing(WindowEvent e)    {	
	this.setVisible(false);
	this.dispose(); 
    }
    public void windowActivated(WindowEvent e)  { }
    public void windowClosed(WindowEvent e)     { }
    public void windowDeactivated(WindowEvent e){ }
    public void windowDeiconified(WindowEvent e){ }
    public void windowIconified(WindowEvent e)  { }
    public void windowOpened(WindowEvent e)     { }


    //
    // MouseListener stubs
    //
    public void mouseClicked(MouseEvent e) 
    {
	//if (e.getClickCount() == 1)
	int index = dtdlist.locationToIndex(e.getPoint());
	dtdlist.setSelectedIndex(index);
	selectedObject = dtdlist.getSelectedValue();
	System.out.println("Selected Item " + selectedObject);
    }
    public void mouseEntered(MouseEvent e) {}
    public void mousePressed(MouseEvent e) {}
    public void mouseExited(MouseEvent e) {}
    public void mouseReleased(MouseEvent e) {}

}







