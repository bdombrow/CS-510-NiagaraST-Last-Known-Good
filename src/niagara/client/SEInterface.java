
/**********************************************************************
  $Id: SEInterface.java,v 1.2 2002/10/12 20:10:25 tufte Exp $


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
import java.util.*;
import java.net.URL;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;

public class SEInterface extends JDialog implements ActionListener {
    Container contentPane;
    
    GridBagLayout layout;
    GridBagConstraints layoutC; 

    Vector dtdList;
    StringBuffer seQuery;

    JPanel  iPanel;
    JPanel  bPanel;

    JButton chooseDTDButton;
    JButton okButton;
    JButton cancelButton;

    JTree seTree;
    JScrollPane seScrollPane;

    DefaultTreeModel seTreeModel;

	/**
	 * Query Execution IF to use for getting a dtd
	 */
	QueryExecutionIF queryExecutionIF;

    public SEInterface(Frame frame, boolean f, StringBuffer result, Vector dtdL, QueryExecutionIF qeIF) {
	super(frame,"Search Engine Query Interface",f);
	contentPane = this.getContentPane();
	layout = new GridBagLayout();
	layoutC = new GridBagConstraints();
	contentPane.setLayout(layout);
	
	setSize(500,400);
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();
	
	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.height)/2);
	
	addWindowListener(new WindowAdapter() {
	    public void windowClosing(WindowEvent e) {
		exitSE();
	    }
	});
	
	dtdList = dtdL;
	seQuery = result;
	queryExecutionIF = qeIF;

	createSEInterface();
	setVisible(true);
    }
    
    public void exitSE() {
	setVisible(false);	
    }
    
    public void updateSE() {
	String str = getSEQLQuery();
	seQuery.append(str);		
    }
    
    public void createSEInterface() {
	iPanel = new JPanel(new BorderLayout());
	
	seTreeModel = new DefaultTreeModel(new DefaultMutableTreeNode());
	seTree = new JTree(seTreeModel);
	seTree.setCellRenderer(new SECellRenderer());
	seTreeModel.addTreeModelListener(new MyTreeModelListener());
	seScrollPane = new JScrollPane(seTree);
	iPanel.add(seScrollPane);
	
	layoutC.gridx = 0;
	layoutC.gridy = 0;
	layoutC.weightx = 1.0;
	layoutC.weighty = 0.85;
	layoutC.fill = GridBagConstraints.BOTH;
	layout.setConstraints(iPanel,layoutC);
	contentPane.add(iPanel);
	
	GridBagLayout bL = new GridBagLayout();
	GridBagConstraints bLC = new GridBagConstraints();
	
	bPanel = new JPanel(bL);
	Border b = BorderFactory.createLoweredBevelBorder();
	bPanel.setBorder(b);
	
	chooseDTDButton = makeButton("Choose DTD",KeyEvent.VK_D,"Choose DTD");
	bLC.gridx = 0;
	bLC.gridy = 0;
	bLC.weightx = 1.0;
	bLC.weighty = 1.0;
	bLC.anchor = GridBagConstraints.WEST;
	bL.setConstraints(chooseDTDButton,bLC);
	bPanel.add(chooseDTDButton);
	
	okButton = makeButton("OK",KeyEvent.VK_O,"OK");
	bLC.gridx = 1;
	bLC.fill = GridBagConstraints.BOTH;
	bLC.anchor = GridBagConstraints.EAST;
	bL.setConstraints(okButton,bLC);
	bPanel.add(okButton);
	
	cancelButton = makeButton("Cancel",KeyEvent.VK_C,"Cancel");
	bLC.gridx = 2;
	bL.setConstraints(cancelButton,bLC);
	bPanel.add(cancelButton);
	
	layoutC.gridy = 1;
	layoutC.weighty = GridBagConstraints.REMAINDER;
	layoutC.anchor = GridBagConstraints.SOUTH;
	layout.setConstraints(bPanel,layoutC);
	contentPane.add(bPanel);
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
	    updateSE();
	    exitSE();
	}
	
	
	if ( actionCommand.equals(cancelButton.getText()) ) 
	    exitSE();
	
	if ( actionCommand.equals(chooseDTDButton.getText()) ) {
	    ChooseDTD dtd = new ChooseDTD(null,true,dtdList,false);
	    String url = dtd.getOneURLString();

	    try {
		URL dtdURL = new URL(url);
		final DefaultMutableTreeNode dtdTree = queryExecutionIF.generateSETree(dtdURL);
		
		seTreeModel = new DefaultTreeModel(dtdTree);
		seTree = new JTree(seTreeModel);
		seTree.setCellRenderer(new SECellRenderer());
		seTreeModel.addTreeModelListener(new MyTreeModelListener());
		seTree.setVisible(true);
		seScrollPane.getViewport().add(seTree);
		seScrollPane.validate();
		
		final JDialog thisDialog = this;
		
		MouseListener ml = new MouseAdapter() {
		    public void mouseClicked(MouseEvent em) {
			int selRow = seTree.getRowForLocation(em.getX(), em.getY());

			if(selRow != -1) {
			    // get the default mutable tree node
			    DefaultMutableTreeNode n =
				(DefaultMutableTreeNode)(seTree.getLastSelectedPathComponent());   

			    if ( n != null ) {
				// Toggle projection status on right mouse button click
				if ( SwingUtilities.isRightMouseButton(em) ) {
				    DTDSETreeNode seNode = (DTDSETreeNode)(n.getUserObject());
				    
				    if ( seNode.isChecked() )
					seNode.setCheckedFlag(false);
				    else
					seNode.setCheckedFlag(true);
				    
				    seTreeModel.nodeChanged(n);
				}
			    }

			    if(em.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(em) && !SwingUtilities.isRightMouseButton(em) && !SwingUtilities.isMiddleMouseButton(em)) {
				
				new PredicatePanel(thisDialog, n);
				seTreeModel.nodeChanged(n);
				
			    }
			}
		    }
		};
	
		MouseMotionListener mml = new MouseMotionAdapter() {
		    public void mouseMoved(MouseEvent em) {
			int selRow = seTree.getRowForLocation(em.getX(),em.getY());
			if ( selRow != -1 ) {  
			    seTree.setSelectionRow(selRow);   
			}
		    }
		};
		
		seTree.addMouseListener(ml);
		seTree.addMouseMotionListener(mml);
		
	    } catch ( java.net.MalformedURLException ee ) { System.out.println("Malformed URL!"); }
	}
    }
    
    public String getSEQLQuery() {
	DefaultMutableTreeNode tree = getQueryRoot();
	
	if (tree == null) return "";
	
	return generateSEQL(tree);
    }

    private boolean hasSubQuery(DefaultMutableTreeNode tree) {
	if (tree.getChildCount() <= 0) {
	    return false;
	}
	
	Enumeration enu = tree.children();
	DefaultMutableTreeNode node;
	DTDSETreeNode se;
	
	while (enu.hasMoreElements()) {
	    
	    node = (DefaultMutableTreeNode)enu.nextElement();
	    se = (DTDSETreeNode)(node.getUserObject());
	    
	    if (se != null && se.isChecked()) {
		return true;
	    } else if (hasSubQuery(node)) return true;
	}
	return false;
    }

	
    private String generateSEQL(DefaultMutableTreeNode tree) {
	DTDSETreeNode se = (DTDSETreeNode)(tree.getUserObject());

	if (se == null) return "";

	boolean multiple=false;
	String query, str, pred;

	if (se.isChecked()) {
	    query = se.getName();
	    pred = se.getPredicate();
	    
	    if (pred !=null) 
		pred = pred.trim();
	    
	    if (!hasSubQuery(tree)) {
		if (pred != null && !pred.equals("")) {
		    query += " "+pred;
		}
		return query;
	    } else if (pred !=null && !pred.equals("")) {
		query += " "+pred;

		return query;
	    } else {
		
		query += " contains (";
		Enumeration enu = tree.children();

		DefaultMutableTreeNode node = (DefaultMutableTreeNode)enu.nextElement();
		str = generateSEQL(node);
		if (!str.equals("")) {
		    query += str;
		    multiple = true;
		}
		
		while (enu.hasMoreElements()) {
		    node = (DefaultMutableTreeNode)enu.nextElement();
		    str = generateSEQL(node);
		    if (!str.equals("")) {
			if (multiple) {
			    query += " and " +str;
			} else {
			    query += str;
			}
			
			multiple = true;
		    }
		}

		query += ")";

		return query;

	    }
	} else {
	    query ="";

	    if (tree.getChildCount() > 0) {
		Enumeration enu = tree.children();
	    
		DefaultMutableTreeNode node = (DefaultMutableTreeNode)enu.nextElement();
		str = generateSEQL(node);
		if (!str.equals("")) {
		    query += str;
		    multiple = true;
		}
		
		while (enu.hasMoreElements()) {
		    node = (DefaultMutableTreeNode)enu.nextElement();
		    str = generateSEQL(node);
		    
		    if (!str.equals("")) {
			if (multiple) {
			    query += " and " +str;
			} else {
			    query += str;
			}
			
			multiple = true;
		    }
		}
	    }
	    
	    return query;
	}
    }


	/////////////////////////////////////////////////////
	// USEFUL
	/////////////////////////////////////////////////////
	/**
	 * Get the root of the tree
	 */
    public DefaultMutableTreeNode getQueryRoot() {
	TreeModel model = seTree.getModel();
	DefaultMutableTreeNode n = (DefaultMutableTreeNode)(model.getRoot());
	
	return n;
    }
    
    public class SECellRenderer extends DefaultTreeCellRenderer {
	ImageIcon seIcon;
	
	public SECellRenderer() {
	}
	
	public Component getTreeCellRendererComponent(
						      JTree tree,
						      Object value,
						      boolean sel,
						      boolean expanded,
						      boolean leaf,
						      int row,
						      boolean hasFocus) {
	    
	    Image image = Toolkit.getDefaultToolkit().getImage(this.getClass().getResource("images/blueball.gif"));
	    seIcon = new ImageIcon(image);
	    
	    super.getTreeCellRendererComponent(
					       tree, value, sel,
					       expanded, leaf, row,
					       hasFocus);
	    
	    DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
	    
	    DTDSETreeNode seNode = (DTDSETreeNode)node.getUserObject();
	    
	    if ( seNode != null ) {
		if( seNode.getPredicate() != null ) {
		    seTree.setEditable(true);
		    setText(seNode.getName() + " " + seNode.getPredicate());
		    seTree.setEditable(false);
		} else
		    setText(seNode.getName());

		if ( seNode.isChecked() ) {
		    image = Toolkit.getDefaultToolkit().getImage(this.getClass().getResource("images/redball.gif"));
		    seIcon = new ImageIcon(image);
		}
	    }
	    
	    setIcon(seIcon);

	    return this;
	}
    }
    
    class MyTreeModelListener implements TreeModelListener {
        public void treeNodesChanged(TreeModelEvent e) {
            DefaultMutableTreeNode node;
            node = (DefaultMutableTreeNode)
		(e.getTreePath().getLastPathComponent());
            try {
                int index = e.getChildIndices()[0];
                node = (DefaultMutableTreeNode)
		    (node.getChildAt(index));
            } catch (NullPointerException exc) {}
        }
        public void treeNodesInserted(TreeModelEvent e) {
        }
        public void treeNodesRemoved(TreeModelEvent e) {
        }
        public void treeStructureChanged(TreeModelEvent e) {
        }
    }
}
