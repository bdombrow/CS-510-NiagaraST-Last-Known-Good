
/**********************************************************************
  $Id: SEURLContent.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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
import java.util.Vector;
import java.util.Date;
import java.net.URL;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;

/**
 * Display a url
 */


public class SEURLContent implements ActionListener, Runnable {

	JFrame frame;
	Container urlContentPane;

	GridBagLayout urlLayout;
	GridBagConstraints urlLayoutC;

	JTextArea urlText;
	JScrollPane urlScrollPane;
	JButton urlOKButton;

	String url = null;

	SEURLContent(final String urlString) {
		
		url = urlString;
		
		frame = new JFrame("XML Document::"+urlString);
	    urlContentPane = frame.getContentPane();

	    urlLayout = new GridBagLayout();
	    urlLayoutC = new GridBagConstraints();
	    urlContentPane.setLayout(urlLayout);

	    frame.setSize(550,350);
	    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	    Rectangle frameBounds = frame.getBounds();

	    frame.setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);

	    // For closing of window(frame)
	    frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				// Call exitUI method to exit gracefully
				exitURL();
			}
	    });

		new Thread(this).start();
	}

	public void run()
		{
			createURL(url);
		}

	public void createURL(String urlString) {
	    StringBuffer content = new StringBuffer();

	    try {
			URL url = new URL(urlString);
		
			InputStream urlStream = url.openStream();
			BufferedReader urlReader = new BufferedReader(new InputStreamReader(urlStream));
			
			String buf = "";
			buf = urlReader.readLine();
			while (buf!=null){
				content.append(buf+"\n");
				buf = urlReader.readLine();
			}
		
	    } 
		catch (java.net.MalformedURLException e) 
		{ return; }
		catch (java.io.IOException io) 
		{ return; }
	    
	    GridBagLayout urlP = new GridBagLayout();
	    GridBagConstraints urlPanelC = new GridBagConstraints();

	    JPanel urlPanel = new JPanel(urlP);
	    
	    urlText = new JTextArea(content.toString());
	    urlText.setCaretPosition(0);
	    urlText.setEditable(false);
	    
	    urlScrollPane = new JScrollPane(urlText);
	    urlPanelC.gridx = 0;
	    urlPanelC.gridy = 0;
	    urlPanelC.weightx = 1.0;
	    urlPanelC.weighty = 1.0;
	    urlPanelC.fill = GridBagConstraints.BOTH;
	    urlP.setConstraints(urlScrollPane,urlPanelC);
	    urlPanel.add(urlScrollPane);
	    
	    JPanel buttonPanel = new JPanel(urlP);
	    Border bb = BorderFactory.createLoweredBevelBorder();
	    buttonPanel.setBorder(bb);
	    
	    urlOKButton = new JButton("Close");
	    urlOKButton.setMnemonic(KeyEvent.VK_C);
	    urlPanelC.gridy = 1;
	    urlP.setConstraints(urlOKButton,urlPanelC);
	    urlOKButton.addActionListener(this);
	    buttonPanel.add(urlOKButton);
	    
	    urlLayoutC.gridy = 0;
	    urlLayoutC.weightx = 1.0;
	    urlLayoutC.weighty = 0.98;
	    urlLayoutC.fill = GridBagConstraints.BOTH;
	    urlLayout.setConstraints(urlPanel, urlLayoutC);
	    urlContentPane.add(urlPanel);
	    
	    urlLayoutC.gridy = 1;
	    urlLayoutC.weighty = 0.02;
	    urlLayout.setConstraints(buttonPanel, urlLayoutC);
	    urlContentPane.add(buttonPanel);

		frame.setVisible(true);
	}

	public void exitURL() {
	    frame.setVisible(false);
	    frame.dispose();
	}

	public void actionPerformed(ActionEvent e) {
	    if ( e.getActionCommand().equals(urlOKButton.getText()) ) 
			exitURL();
	}
}
