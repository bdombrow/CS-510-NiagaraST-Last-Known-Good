
/**********************************************************************
  $Id: DNDTree.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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
import java.awt.dnd.*;
import java.awt.datatransfer.*;

import java.util.Hashtable;
import java.util.List;
import java.util.Iterator;

import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;
import niagara.client.dtdTree.*;

public class DNDTree extends JTree
implements DropTargetListener,DragSourceListener, DragGestureListener {
  
    private DropTarget dropTarget = null;  
    private DragSource dragSource = null;

    private static DefaultMutableTreeNode startJoinNode;
    private static DefaultTreeModel startModel;

    private static StringBuffer joinString;
    private static JoinColor joinColor;

    private static Color currentJoinColor;

    public DNDTree(DefaultMutableTreeNode dtdTree, XmlInterface dialog, StringBuffer jString, JoinColor jColor) {
		final DefaultTreeModel xmlTreeModel = new DefaultTreeModel(dtdTree);
		xmlTreeModel.addTreeModelListener(new MyTreeModelListener());
    
		this.setModel(xmlTreeModel);
		this.setCellRenderer(new XmlCellRenderer());

		joinString = jString;
		joinColor = jColor;

		final JTree xmlTree = this;
		final JDialog thisDialog = dialog;
		final XmlInterface xmlqbe = dialog;

		MouseListener ml = new MouseAdapter() {
			public void mouseClicked(MouseEvent em) {
				int selRow = xmlTree.getRowForLocation(em.getX(), em.getY());
				if(selRow != -1) {
					// get the default mutable tree node
					DefaultMutableTreeNode n =
					(DefaultMutableTreeNode)(xmlTree.getLastSelectedPathComponent());   
					if ( n != null ) {
						// Toggle projection status on right mouse button click
						if ( SwingUtilities.isRightMouseButton(em) ) {
							DTDXMLQLTreeNode xmlNode = (DTDXMLQLTreeNode)(n.getUserObject());
							if ( xmlNode.isProjected() ) {
								xmlNode.setProjection(false);
								xmlqbe.decProjectCount();
							}
							else {
								xmlNode.setProjection(true);
								xmlqbe.incProjectCount();
							}
			    
							xmlTreeModel.nodeChanged(n);
						}
					}
		    
					// Show predicate window if the left mouse button was double-clicked
					if ( em.getClickCount() == 2 && SwingUtilities.isLeftMouseButton(em) && !SwingUtilities.isRightMouseButton(em) && !SwingUtilities.isMiddleMouseButton(em) ) {
						if ( isLeafElement(n) ) {
							new PredicateGUI(thisDialog, n);
							xmlTreeModel.nodeChanged(n);		
						}
					}    
				}
			}

			private boolean isLeafElement(DefaultMutableTreeNode n){
				// get the nodes children
				Enumeration children = n.children();

				boolean allAttributes = true;
				
				while(children.hasMoreElements()){
					DefaultMutableTreeNode node = (DefaultMutableTreeNode)(children.nextElement());
					DTDXMLQLTreeNode nodeContent= (DTDXMLQLTreeNode)(node.getUserObject());
					allAttributes = allAttributes && nodeContent.isAttribute();
				}
				
				return allAttributes;
			}
		};
	
		MouseMotionListener mml = new MouseMotionAdapter() {
			public void mouseMoved(MouseEvent em) {
				int selRow = xmlTree.getRowForLocation(em.getX(),em.getY());
				if ( selRow != -1 ) {  
					xmlTree.setSelectionRow(selRow);   
				}
			}
		};

		this.setSelectionRow(0);
		this.addMouseListener(ml);
		this.addMouseMotionListener(mml);
		this.setVisible(true);
	
		dropTarget = new DropTarget (this, this);
		dragSource = new DragSource();
		dragSource.createDefaultDragGestureRecognizer(this, DnDConstants.ACTION_COPY, this);
    }

    /**
     * is invoked when you are dragging over the DropSite
     * 
     */
    public void dragEnter(DropTargetDragEvent event) {
		event.acceptDrag(DnDConstants.ACTION_COPY);
    }
    
    /**
     * is invoked when you are exit the DropSite without dropping
     *
     */
    public void dragExit(DropTargetEvent event) {    	 
    }
    
    /**
     * is invoked when a drag operation is going on
     * 
     */
    public void dragOver(DropTargetDragEvent event) {
		int selRow = this.getRowForLocation((int)event.getLocation().getX(), (int)event.getLocation().getY());
		if ( selRow != -1 ) {
			this.setSelectionRow(selRow);
		}
    }

    /**
     * a drop has occurred
     * 
     */ 
    public void drop(DropTargetDropEvent event) {    
		try {
			Transferable transferable = event.getTransferable();
	    
			event.acceptDrop(DnDConstants.ACTION_COPY);
			String s = (String)transferable.getTransferData(DataFlavor.stringFlavor);
	    
			DefaultMutableTreeNode n =
				(DefaultMutableTreeNode)(this.getLastSelectedPathComponent());  

			DTDXMLQLTreeNode xmlNode = (DTDXMLQLTreeNode)(n.getUserObject());

			DefaultTreeModel treeModel = (DefaultTreeModel)this.getModel();

			if ( !treeModel.equals(startModel) && n.isLeaf() ) {
				JoinType joinType = new JoinType(s,xmlNode.getVariableName());
				Point point = event.getLocation();
				joinType.showPopUp(this,(int)point.getX(),(int)point.getY());

				joinString.append(", "+s+" = "+xmlNode.getVariableName());

				xmlNode.setJoined();
				xmlNode.setJoinColor(currentJoinColor);
				((DefaultTreeModel)(this.getModel())).nodeChanged(n);
				event.getDropTargetContext().dropComplete(true);  
			}
			else {
				Toolkit.getDefaultToolkit().beep();
				JOptionPane.showMessageDialog(null,"Invalid Join Operation!","Niagara",JOptionPane.ERROR_MESSAGE);   
				event.rejectDrop();
			}
		}
		catch (IOException exception) {
			event.rejectDrop();
		} 
		catch (UnsupportedFlavorException ufException ) {
			event.rejectDrop();
		}
    }
    
    /**
     * is invoked if the use modifies the current drop gesture
     * 
     */    
    public void dropActionChanged ( DropTargetDragEvent event ) {
    }
    
    /**
     * a drag gesture has been initiated
     * 
     */
    
    public void dragGestureRecognized(DragGestureEvent event) {
		startJoinNode  =
			(DefaultMutableTreeNode)(this.getLastSelectedPathComponent());  

		if ( startJoinNode.isLeaf() ) {
			DTDXMLQLTreeNode xmlNode = (DTDXMLQLTreeNode)(startJoinNode.getUserObject());
			startModel = (DefaultTreeModel)this.getModel();

			StringSelection text = new StringSelection(xmlNode.getVariableName());

			if ( xmlNode.getJoinColor() == null ) {
				currentJoinColor = joinColor.getJoinColor();
			}
			else
				currentJoinColor = xmlNode.getJoinColor();

			dragSource.startDrag(event, DragSource.DefaultLinkDrop, text, this);
		}
    }
    
    /**
     * this message goes to DragSourceListener, informing it that the dragging 
     * has ended
     * 
     */
    
    public void dragDropEnd (DragSourceDropEvent event) {   
		if ( event.getDropSuccess()){
			DTDXMLQLTreeNode xmlNode = (DTDXMLQLTreeNode)(startJoinNode.getUserObject());
	    
			xmlNode.setJoined();
			xmlNode.setJoinColor(currentJoinColor);
			((DefaultTreeModel)(this.getModel())).nodeChanged(startJoinNode);
		}
    }
    
    /**
     * this message goes to DragSourceListener, informing it that the dragging 
     * has entered the DropSite
     * 
     */    
    public void dragEnter (DragSourceDragEvent event) {
    }
    
    /**
     * this message goes to DragSourceListener, informing it that the dragging 
     * has exited the DropSite
     * 
     */
    public void dragExit (DragSourceEvent event) {
    }
    

    /**
     * this message goes to DragSourceListener, informing it that the dragging is currently 
     * ocurring over the DropSite
     * 
     */
    public void dragOver (DragSourceDragEvent event) {	
    }
    
    /**
     * is invoked when the user changes the dropAction
     * 
     */
    public void dropActionChanged (DragSourceDragEvent event) {
    } 

    class XmlCellRenderer extends DefaultTreeCellRenderer {
		ImageIcon xmlIcon;
	
		public Component getTreeCellRendererComponent(
			JTree tree,
			Object value,
			boolean sel,
			boolean expanded,
			boolean leaf,
			int row,
			boolean hasFocus) {
	    
			// Default bullet for tree nodes
			Image image = Toolkit.getDefaultToolkit().getImage(this.getClass().getResource("blueball.gif"));
			xmlIcon = new ImageIcon(image);
	    
			super.getTreeCellRendererComponent(
				tree, value, sel,
				expanded, leaf, row,
				hasFocus);
	    
			DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
	    
			DTDXMLQLTreeNode xmlNode = (DTDXMLQLTreeNode)node.getUserObject();
	    
			if ( xmlNode != null ) {
				// Set predicate value
				if( xmlNode.hasPredicate() ) {
					tree.setEditable(true);
					StringTokenizer predicate = new StringTokenizer(xmlNode.getPredicate());
					StringBuffer text = new StringBuffer();
					String varName = xmlNode.getVariableName();
					String name = xmlNode.getName();

					while ( predicate.hasMoreTokens() ) {
						String token = predicate.nextToken();

						if ( token.equals(varName) )
							text.append(name+" ");
						else
							text.append(token+" ");
					}
					setText(text.toString());
					tree.setEditable(false);
				}
				else 
					setText(xmlNode.getName());
		
				// Set projection status
				if ( xmlNode.isProjected() ) {
					image = Toolkit.getDefaultToolkit().getImage(this.getClass().getResource("redball.gif"));
					xmlIcon = new ImageIcon(image);
				}
				else
					if( xmlNode.isAttribute() ) {
						image = Toolkit.getDefaultToolkit().getImage(this.getClass().getResource("yellowball.gif"));
						xmlIcon = new ImageIcon(image);
					}
		
				if ( xmlNode.isJoined() )
					//setForeground(xmlNode.getJoinColor());
					this.setForeground(xmlNode.getJoinColor());
			}
	    
			setIcon(xmlIcon);
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

    class JoinType extends JPopupMenu implements ActionListener {
		private String joinType;
		private String var1;
		private String var2;

		JoinType(String first, String second) {
			JMenuItem item = makeItem("Join Type");
			item.setEnabled(false);
			this.add(item);

			this.addSeparator();

			this.add(makeItem("="));
			this.add(makeItem("<"));
			this.add(makeItem(">"));
			this.add(makeItem("<="));
			this.add(makeItem(">="));

			setDefaultLightWeightPopupEnabled(true);

			var1 = first;
			var2 = second;
		}

		private JMenuItem makeItem(String item) {
			JMenuItem menuItem = new JMenuItem(item);
			menuItem.addActionListener(this);
			return menuItem;
		}

		public void showPopUp(Component component, int x, int y) {
			this.show(component,x,y);
		}

		public void actionPerformed(ActionEvent e) {
			joinType = e.getActionCommand();

			int startIndex;
			String lookFor = new String(var1+" = "+var2);

			if ( (startIndex = joinString.toString().indexOf(lookFor)) != -1 ) {
				String newJoinCase = new String(var1+" "+joinType+" "+var2);
				int endIndex = joinString.toString().length();

				joinString.replace(startIndex,endIndex,newJoinCase);
			}
		}

		public String getJoinType() {
			return joinType;
		}
    }
}
