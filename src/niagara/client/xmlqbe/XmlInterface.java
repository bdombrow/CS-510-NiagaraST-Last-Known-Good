
/**********************************************************************
  $Id: XmlInterface.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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


package niagara.client.xmlqbe;

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.*;
import java.net.URL;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;
import niagara.client.ChooseDTD;

import niagara.client.QueryExecutionIF;    // for getting dtd's from the server

public class XmlInterface extends JDialog implements ActionListener {

    /**
     * to use for getting the dtds
     */
    private QueryExecutionIF queryExecutionIF;
    
    private Container contentPane;
    
    private GridBagLayout layout;
    private GridBagConstraints layoutC; 

    private GridLayout treeLayout;

    private JScrollPane scrollPane;
    
    private JDesktopPane treePanel;

    private JButton okButton;
    private JButton cancelButton;
    private JButton chooseDTDButton;
    private JButton joinButton;
    private JButton cancelJoinButton;
    
    private Vector dtdList;

    private StringBuffer xmlQuery;

    private XMLQLGenerationData xmlqlData;
    private JoinColor joinColor;
    private StringBuffer joinString;

    private static final int frameWidth = 225;
    private static final int frameHeight = 250;

    private int locX;
    private int locY;
 
    private static final int PAD = 10;
    private int gridXLocation = 0;

    private int projectCount;

    public XmlInterface(Frame frame, boolean f, StringBuffer result, Vector dtdL, QueryExecutionIF qeIF) {
	super(frame,"XML Query Interface",f);
	contentPane = this.getContentPane();
	layout = new GridBagLayout();
	layoutC = new GridBagConstraints();
	contentPane.setLayout(layout);
	
	setSize(750,425);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.height)/2);
	
	addWindowListener(new WindowAdapter() {
	    public void windowClosing(WindowEvent e) {
		exitXml();
	    }
	});
	
	dtdList = dtdL;
	queryExecutionIF = qeIF;
	xmlQuery = result;

	joinString = new StringBuffer();
	joinColor = new JoinColor();

	xmlqlData = new XMLQLGenerationData();
	
	locX = PAD;
	locY = PAD;

	projectCount = 0;

	createXmlInterface();
	setVisible(true);
     }
    
    public void exitXml() {
	setVisible(false);
    }

    public void createXmlInterface() {
	chooseDTDButton = makeButton("Choose DTD",KeyEvent.VK_D,"Choose DTD");
	layoutC.gridx = 0;
	layoutC.gridy = 0;
	layoutC.anchor = GridBagConstraints.NORTHWEST;
	layoutC.insets = new Insets(PAD,PAD,PAD,PAD);
	layout.setConstraints(chooseDTDButton,layoutC);
	contentPane.add(chooseDTDButton);

	okButton = makeButton("OK",KeyEvent.VK_O,"OK");
	layoutC.gridx = 1;
	layoutC.gridy = 0;
	layoutC.weightx = 1.0;
	layoutC.anchor = GridBagConstraints.NORTHEAST;
	layout.setConstraints(okButton,layoutC);
	contentPane.add(okButton);

	cancelButton = makeButton("Cancel",KeyEvent.VK_C,"Cancel");
	layoutC.gridx = 2;
	layoutC.gridy = 0;
	layoutC.weightx = 0.0;
	layout.setConstraints(cancelButton,layoutC);
	contentPane.add(cancelButton);

	okButton.setPreferredSize(cancelButton.getPreferredSize());

	treePanel = new JDesktopPane();
	treePanel.putClientProperty("JDesktopPane.dragMode", "outline");
	treePanel.setPreferredSize(new Dimension(1600,1200));

	scrollPane = new JScrollPane(treePanel); 

	layoutC.gridx = 0;
	layoutC.gridy = 1;
	layoutC.weightx = 1.0;
	layoutC.weighty = 1.0;
	layoutC.anchor = GridBagConstraints.CENTER;
	layoutC.insets = new Insets(0,PAD,PAD,PAD);
	layoutC.gridwidth = GridBagConstraints.REMAINDER;
	layoutC.fill = GridBagConstraints.BOTH;
	layout.setConstraints(scrollPane,layoutC);
	contentPane.add(scrollPane);
    }

    // Method to create a button
    private JButton makeButton(String buttonText, int keyBoardMnemonic, String toolTipText) {
        JButton button = new JButton(buttonText);
	button.setMnemonic(keyBoardMnemonic);
	button.setToolTipText(toolTipText);
	button.addActionListener(this);
	return button;
    }    

    public void actionPerformed(ActionEvent e) {
	String actionCommand = e.getActionCommand();

	if ( actionCommand.equals(okButton.getText()) ) {	 

	    if ( projectCount <= 0 ) {
		Toolkit.getDefaultToolkit().beep();
		JOptionPane.showMessageDialog(null,"No Element Projected!","Niagara",JOptionPane.ERROR_MESSAGE);   
		return;
	    }

	    String[] urls = xmlqlData.getURLArray();

	    if ( urls != null ) {
		XMLQLGenerator xmlql = new XMLQLGenerator(xmlqlData.getTreeArray(),urls,joinString);
		xmlQuery.append(xmlql.generateQuery());
	    }

	    exitXml();
	}

	if ( actionCommand.equals(cancelButton.getText()) ) {
	    exitXml();
	}

	if ( actionCommand.equals(chooseDTDButton.getText()) ) {
	    ChooseDTD dtd = new ChooseDTD(null,true,dtdList,true);
	    String[] url = dtd.getURLString();

	    if ( url == null ) return;

	    for ( int i = 0 ; i < url.length ; i++ ) {
		try {
		    URL dtdURL = new URL(url[i]);
		    final DefaultMutableTreeNode dtdTree = queryExecutionIF.generateXMLQLTree(dtdURL);
		
		    if ( !xmlqlData.containsURL(url[i]) ) {
			xmlqlData.addTree(dtdTree);
			xmlqlData.addURL(url[i]);
		    }
		    
		    DNDTree xmlTree = new DNDTree(dtdTree, this, joinString, joinColor);
		      
		    JScrollPane xmlScrollPane = new JScrollPane(xmlTree);
		    Border border = BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(Color.black), BorderFactory.createEtchedBorder());
		    xmlScrollPane.setBorder(border);
		    
		    TreeFrame frame = new TreeFrame(dtdURL.getFile(),xmlScrollPane,locX,locY);
		    
		    treePanel.add(frame);
		    
		    scrollPane.revalidate();
		    
		    locX += (frameWidth+PAD);
		    
		} catch ( java.net.MalformedURLException ee ) { System.out.println("Malformed URL!"); }
	    }
	}
    }

    public void incProjectCount() {
	projectCount++;
    }

    public void decProjectCount() {
	projectCount--;
    }

    class TreeFrame extends JInternalFrame implements InternalFrameListener {	
	TreeFrame(String title,JScrollPane xmlScrollPane, int x, int y) {
	    super(title,false,false,false,false);
	    this.getContentPane().add(xmlScrollPane);
	    this.setBounds(x,y,frameWidth,frameHeight);
	    this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
	    this.addInternalFrameListener(this);
	    this.setVisible(true);
	}
	
	public void internalFrameClosing(InternalFrameEvent e) {
	    locX = this.getX();
	    locY = this.getY();
	}
	
	public void internalFrameClosed(InternalFrameEvent e) {
	}
	
	public void internalFrameOpened(InternalFrameEvent e) {
	}
	
	public void internalFrameIconified(InternalFrameEvent e) {
	}
	
	public void internalFrameDeiconified(InternalFrameEvent e) {
	}

	public void internalFrameActivated(InternalFrameEvent e) {
	}
	
	public void internalFrameDeactivated(InternalFrameEvent e) {
	}
    }
}
