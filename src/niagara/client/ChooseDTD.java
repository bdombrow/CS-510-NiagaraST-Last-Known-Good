
/**********************************************************************
  $Id: ChooseDTD.java,v 1.1 2000/05/30 21:03:24 tufte Exp $


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

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;

public class ChooseDTD extends JDialog implements ActionListener {
    private Container cPane;
    
    private JPanel  iPanel;
    private  JPanel  bPanel;
    
    private  JButton okButton;
    private JList   dtdList;
    
    private  JScrollPane iScrollPane;
    
    private  DefaultListModel listModel;
    
    private  GridBagLayout layout;
    private  GridBagConstraints layoutC;
    
    private String[]  urlString;
    private String    oneUrlString;

    private boolean multipleSelection;
    
    public ChooseDTD(Frame frame, boolean f, Vector dtds, boolean selection) {
	super(frame,"Choose DTD",f);
	cPane = this.getContentPane();
	layout = new GridBagLayout();
	layoutC = new GridBagConstraints();
	cPane.setLayout(layout);
	
	setSize(400,225);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);
	
	addWindowListener(new WindowAdapter() {
	    public void windowClosing(WindowEvent e) {
		exitDTD();
	    }
	});	    
	
	multipleSelection = selection;

	displayDTD();
	
	for ( int i = 0 ; i < dtds.size() ; i++ ) {
	    listModel.addElement(dtds.elementAt(i));
	}
	
	pack();
	setVisible(true);
    }
    
    public void displayDTD() {
	iPanel = new JPanel(new BorderLayout());
	
	listModel = new DefaultListModel();
	dtdList = new JList(listModel);

	if ( multipleSelection ) 
	    dtdList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
	else
	     dtdList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

	iScrollPane = new JScrollPane(dtdList);
	iPanel.add(iScrollPane);
	
	layoutC.gridx = 0;
	layoutC.gridy = 0;
	layoutC.weightx = 1.0;
	layoutC.weighty = 0.85;
	layoutC.fill = GridBagConstraints.BOTH;
	layout.setConstraints(iPanel,layoutC);
	cPane.add(iPanel);
	
	GridBagLayout bL = new GridBagLayout();
	GridBagConstraints bLC = new GridBagConstraints();
	
	bPanel = new JPanel(bL);
	Border b = BorderFactory.createLoweredBevelBorder();
	bPanel.setBorder(b);
	
	okButton = new JButton("OK");
	okButton.setMnemonic(KeyEvent.VK_O);
	okButton.setToolTipText("OK");
	okButton.addActionListener(this);
	bLC.gridx = 0;
	bLC.gridy = 0;
	bLC.weightx = 1.0;
	bLC.weighty = 1.0;
	bLC.fill = GridBagConstraints.BOTH;
	bL.setConstraints(okButton,bLC);
	bPanel.add(okButton);
	
	layoutC.gridy = 1;
	layoutC.weighty = GridBagConstraints.REMAINDER;
	layoutC.anchor = GridBagConstraints.SOUTH;
	layout.setConstraints(bPanel,layoutC);
	cPane.add(bPanel);    
    }
    
    public void exitDTD() {
	setVisible(false);
    }
    
    public void update(Graphics g) {
	super.update(g);
	dtdList.revalidate();
    }

    public void actionPerformed(ActionEvent e) {
	String actionCommand = e.getActionCommand();
	
	if ( actionCommand.equals(okButton.getText()) ) {
	    if ( multipleSelection ) {
		Object[] urls = dtdList.getSelectedValues();
		urlString = new String[urls.length];
		
		for ( int i = 0 ; i < urls.length ; i++ )
		    urlString[i] = (String)urls[i];
	    }
	    else {
		oneUrlString = (String)dtdList.getSelectedValue();
	    }
	    
	    exitDTD();
	}
    }
    
    public String[] getURLString() {
	return urlString;
    }

    public String getOneURLString() {
	return oneUrlString;
    }
}
