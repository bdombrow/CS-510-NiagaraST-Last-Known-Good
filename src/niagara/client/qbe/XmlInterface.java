
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


package niagara.client.qbe;
import java.awt.*;
import java.awt.event.*;
import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.io.*;
import java.util.*;
import javax.swing.*;
import javax.swing.border.*;
import niagara.utils.*;

/***************************************************************
 *                                                             *
 *  createFrame()                    create the whole frame    *
 *  init()                                                     *
 *  initTreePanel()                                            *
 *  drawTree()                                                 *
 *  redrawTree()                                               *
 *  actionListen()                   add the action listener   *
 *  drawButton_action()                                        *
 *  projButton_action_performed(ObjectDes objectDes)           *
 *  loadButton_action()                                        *
 *  resetButton_action()                                       *
 *  goButton_action()					       *
 *  showButton_action()					       *
 *  paint(Graphics g)                paint                     *
 *  setQueryResult(String queryTxt)                            *
 *  setWarnMsg(String warnTxt)                                 *
 *  getRoot_Leaf_Attr()                                        *
 *  getRootElements()                                          *
 *  getLeafAttr()                                              *
 *  isLeaf()                                                   *
 *  isAttr()                                                   *
 *                                                             *
 ***************************************************************/

/**
 *  This class creates and displays the XML-QL Query By Example dialog frame.
 *  The graphical query generator was written as a 764 project, Spring 99.
 *  Modifications were made to integrate this gui with the Niagra front end.
 *  Most of the comments are part of those modifications
 *  
 */
public class XmlInterface extends JFrame
{
    
    // The actual dialog window
    //
    public XmlInterfaceDialog xmlDialog;
    
    // A result buffer which will receive the XML-QL query
    //
    public StringBuffer resultBuffer;

    // A list of DTD urls to display in the DTD selection window
    //
    public Vector dtdList;

    /**
     *  Constructor: create an xmlDialog object and display it
     *  
     *  @param resultBuff the buffer to write the resulting XML-QL query to
     *  @param dtdList the list of known dtds (these are displayed in the window for choosing dtds)
     */
    public XmlInterface(StringBuffer resultBuf, Vector dtdList)
    {
	
	this.resultBuffer = resultBuf;
	this.dtdList = dtdList;
	
	xmlDialog = new XmlInterfaceDialog(this, 
					   "XML-QL Query By Example Interface", resultBuffer, dtdList);
	
	xmlDialog.setModal(true);
	xmlDialog.setBounds(0, 0, 710, 530);  //810 630
	xmlDialog.setBackground(Color.white);
	xmlDialog.setVisible(true);
    }
}


/**
 *  This class is the gui frame displayed when an XmlInterface object is created.
 *  It is the main window the user interacts with to generate an XML-QL query by
 *  graphically manipulating DTDs, applying predicates, choosing projections etc.
 *  The code was written by a 764 group, and modified to use all swing 
 *  components and to integrate with Niagra
 *  
 */
class XmlInterfaceDialog extends JDialog
{

    //StringBuffer buffer;
    public Vector dtdList;                 // List of DTDs to display for the user to choose from
    public StringBuffer resultBuffer;      // The buffer to receive the XML-QL query

    // Various fixed fonts used in GUI componenets
    //
    public Font buttonFont;
    public Font labelFont;
    public Font predLabelFont;


    /* ------------- member definition ------------------*/
    final int InterfaceWidth  = 700;      // 800
    final int ButtonWidth     = 110;       // button width 110
    final int ButtonHeight    = 20;       // button height 25

    final int ButtonPaneHt    = 45-2;     // panel for displaying buttons 50
    final int TreePaneHt      = 400-2;    // panel for displaying XML tree 500
    final int JoinPaneHt      = 45-2;     // panel for displaying join button 50
    final int TreeDistance    = 25; // 30
    final int TreeWidth       = 300; // 325
    final int TreeHeight      = 330; // 430

    final int SUB_PANEL_WIDTH = 1000;     // max panel with on scrollpane
    final int SUB_PANEL_HEIGHT= 5000;     // max panel height on scrollpane
    final int MAX_PANEL_WIDTH = 9000;     // max panel with on scrollpane
    final int MAX_PANEL_HEIGHT= 5000;     // max panel height on scrollpane

    JButton loadButton;                   // load XML button
    JButton resetButton;                  // reset button
    JButton goButton;                     // go button
    JButton showButton;                   // show XML-QL button
    JButton goExitButton;                 // go and exit button

    ActionLn actionLn;                    // action listener
    SymMouse mouseLn;                     // mouse listener


    Vector dtdNames;                      // xml file name vector
    int    DTDindex;                      // index for the that vector

    Vector dtdElementsVect;               // all the elements in a DTD
    Vector rootElementsVect;              // elements that are roots in DTDs
    Vector leafContentsVect;              // all leaf elements
    Vector attrListVect;                  // attribute list

    XmlQlFrame xmlQlFrame;
    String     xmlQlStr;                  // xml query String

    JPanel buttonPanel;
    JPanel treePanel;
    JPanel joinPanel;
    JScrollPane treeScroll;

    Vector xmlTreePanelVect;              // vector of all the tree panels
    Vector xmlTreeScrollVect;             // vector of all the tree panel scrollPanes
    Vector xmlTreeStruVect;               // vector of the structure of the xml tree

    final int LineHeight = 25;//30            // each line height in the xml tree
    final int IndentWidth = 20;           // indent width of each indent level

    final int HorizonDist = 5;
    final int OpenButtonWidth = 20; 
    final int OpenButtonHeight = 18;
    final int ProjButtonRadius = 15;
    final int LabelWidth = 400;
    final int LabelHeight = 30;

    ImageIcon openIcon;
    ImageIcon closeIcon;
    ImageIcon redIcon;
    ImageIcon blueIcon;
    ImageIcon yellowIcon;

    EmptyBorder emptyBorder;
    EtchedBorder etchedBorder;

    final int OPEN_BUTTON = 0;
    final int PROJ_BUTTON = 1;

    JButton   joinStButton;
    JButton   joinEndButton;
    JButton   joinCancelButton;
    boolean   joinState;                 // true = begin join, false = stop join
    int       joinSeqNum;                // join sequential number
    JoinColor joinColor;                 // join display color

    Vector    joinVect;                  // each entry in vector is a vector of joined parts
    Vector    tempJoins;                 // temporary joins

    final Color bg = Color.white;
    final Color tbg = new Color(230,230,230);
    final Color black = Color.black;
    final Color buttonColor = Color.lightGray;
    Color curJoinColor = Color.black;

    /**
     *  Constructor: create the GUI frame and init the resultBuffer and dtd list
     *      
     *  @param parent the parent of this frame
     *  @param interfaceTit the title of this frame  
     *  @param rb the string buffer that will receive the XML-QL query
     *  @param dtdList the list of DTDs that the user can select from
     */    
    public XmlInterfaceDialog(JFrame parent, String interfaceTit, 
			      StringBuffer rb, Vector dtdList)
    {

	super(parent);
	setTitle(interfaceTit);

	// From QE-Gui
	//
	this.resultBuffer = rb;
	this.dtdList = dtdList;

	// Init commonly used fonts
	//
	buttonFont = new Font("SansSerif", Font.PLAIN, 10);
	labelFont = new Font("SansSerif", Font.PLAIN, 12);
	predLabelFont = new Font("SansSerif", Font.BOLD, 10);

	// Create main frame
	//
	createFrame();

	// Do the event listener
	//
	addEventListener();

	joinState = false;
	joinSeqNum = 1;

	// dtdElements = new Vector();
	//
	DTDindex = 0;
	dtdElementsVect = new Vector();
	rootElementsVect = new Vector();
	leafContentsVect = new Vector();
	attrListVect = new Vector();

	joinColor = new JoinColor();
	joinVect = new Vector();
	tempJoins = new Vector();

	xmlTreePanelVect = new Vector(); 
	xmlTreeScrollVect = new Vector(); 
	xmlTreeStruVect = new Vector();

	dtdNames = new Vector();

	xmlQlStr = null;

	// Init the icons used when displaying dtds
	//
	initIcon();
    }

    
    /**
     *  Initialize the icons used for displaying tree components
     *
     */
    void initIcon()
    {
	// init empty and etched border
	//
	emptyBorder = new EmptyBorder(1,1,1,1);
	etchedBorder = new EtchedBorder();
	
	// icons..  NOTE: Location is hardcoded!!
	//
	try{
	    openIcon   = new ImageIcon("niagara/client/arrowopened.gif");
	    closeIcon  = new ImageIcon("niagara/client/arrowclosed.gif");
	    blueIcon   = new ImageIcon("niagara/client/blueball.gif");
	    redIcon    = new ImageIcon("niagara/client/redball.gif");
	    yellowIcon = new ImageIcon("niagara/client/yellowball.gif");	    
	} catch (Exception e){
	    e.printStackTrace();
	}
    }
    


    /**
     *  Process the event created by clicking on the load button.
     *  This will generate a dialog box that will allow the user to 
     *  select a dtd from a list.  After choosing a dtd from the list
     *  this dtd will be parsed and displayed in a new panel.  This
     *  new panel is added to the treePanel, the panel of the main frame
     *  that contains all dtd trees. 
     *
     */    
    void loadButton_action()
    {

	String xmlFn = new String();

	try
	{
	    // Select a DTD from the dtd list  (Modal frame)
	    //
	    ListSelectorFrame lf = new ListSelectorFrame(dtdList, 
							 "Select a DTD: ",
							 "Choose DTD Dialog");

	    // Get the selection
	    //
	    String newDtd = (String)lf.getSelectedItem();

	    if( newDtd == null ) return;
	    if( newDtd.length() == 0 ) return;
	    newDtd = newDtd.trim();
	    if( newDtd.length() == 0 ) return;


	    // Load the new DTD
	    //
	    dtdNames.addElement(newDtd);
	    loadDTD(newDtd);           

	} catch (Exception e){
	    System.err.println(e);
	}
    }


    /**
     *  Given a DTD url, parse the DTD, create a new panel for the DTD,
     *  add this panel to the treePanel, and graphically display the dtd 
     *  tree in this new panel.
     *
     */        
    private void loadDTD(String dtdURL)
    {
	
	// A vector of elements for a given dtd
	//
	Vector dtdElements = new Vector();

	// Get the parsed DTD
	//
	DTD dtd = CUtil.parseDTD(dtdURL);
	if(dtd == null) return;
	
	// get all the DTD elements
	// ------------------------
	Enumeration elements = dtd.getElementDeclarations();

	// iterate through each element in DTD
	// -----------------------------------
	while( elements.hasMoreElements() )
	{
	    ElementDecl oneElement = (ElementDecl) (elements.nextElement());

	    String eleName = oneElement.getName();
	    Vector eleContents = dtd.makeContentElementList(eleName);
	    Enumeration attributes = dtd.getAttributeDeclarations(eleName);
	    Vector attList = new Vector();
	    while( attributes.hasMoreElements())
	    {
		String currAttr = ( (AttDef) (attributes.nextElement()) ).getName();
		attList.addElement(currAttr);
	    }

	    DTDEleEntry dtdEleEntry;
	    if( eleContents != null )
	    {
		dtdEleEntry = new DTDEleEntry(eleName, eleContents, attList);
	    }
	    else
	    {
		eleContents = new Vector();
		dtdEleEntry = new DTDEleEntry(eleName, eleContents, attList);
	    }

	    dtdElements.addElement(dtdEleEntry);
	}

	// first root, maybe multiple root later
	dtdElementsVect.addElement(dtdElements);

	int rootNum = getRoot_Leaf_Attr();

	// save the number of rootNum dtdElements by pointer
	// add the xml filename for each tree
	// ----------------------------

	// NOTE::
	//  Why are they adding the same thing to a vector multiple times????

	for( int i = 1; i <  rootNum; i++ )
	{
	    dtdElementsVect.addElement(dtdElements);
	    dtdNames.addElement(dtdURL);
	}
	
        // initialize tree panel
	// ---------------------
	initTreePanel(rootNum, dtdURL);

	// draw the xml tree
	// ---------------------
	drawTree();

	DTDindex = DTDindex+rootNum;
    }


    /**
     *
     *
     *
     *
     */    
    private void initTreePanel(int rootNum, String xmlFn)
    {
	
	for( int i = DTDindex; i < rootElementsVect.size(); i++ )
	{

	    // Label for the DTD url
	    //
	    JLabel fnLabel = new JLabel(xmlFn);
	    fnLabel.setFont(labelFont);
	    fnLabel.setBackground(Color.white);
	    fnLabel.setForeground(black);
	    fnLabel.setBounds(0,0, 1000,1000);
	    fnLabel.setVisible(true);

	    // draw the tree
	    // 
	    int x = (i+1)*TreeDistance+i*TreeWidth;
	    int y = (int)(TreeDistance-10);

	    // Add a dtd tree panel to the treeScroll pane 
	    // and add this scroll pane to the main tree scroll pane
	    //
	    JPanel currPanel = new JPanel();
	    currPanel.setLayout(null);
	    //currPanel.setBounds(x, y, SUB_PANEL_WIDTH, SUB_PANEL_HEIGHT );
	    currPanel.setPreferredSize(new Dimension(LabelWidth, 500));

	    currPanel.setBackground(tbg);
	    currPanel.setVisible(true);
	    
	    // Create a scroll pane for the new dtd tree
	    //
	    JScrollPane currScroll = 
		new JScrollPane(currPanel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
				JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);

	    currScroll.setBounds( x, y, TreeWidth, TreeHeight);
	    currScroll.getViewport().setBackground(tbg);
	    currScroll.setBorder(etchedBorder);
	    currScroll.getViewport().setLayout(new ViewportLayout());
	    currScroll.setColumnHeaderView(fnLabel);
	    currScroll.getColumnHeader().setBackground(bg);
	    currScroll.setVisible(true);
 

	    // Add tree panel to the tree panel vector
	    //
	    xmlTreePanelVect.addElement(currPanel);
	    xmlTreeScrollVect.addElement(currScroll);

	    
	    //Rectangle r = treePanel.getBounds();
	    //int width = r.width;
	    //int height = r.height;
	    
	    //width = (int)(TreeWidth*DTDindex*1.5);
	    //System.out.println("TreePanel: w="+width+", h="+height+", x="+r.x+", y="+r.y);

	    // Add scroll panes to the main treePanel
	    //
	    treePanel.add(currScroll);
	    treePanel.setVisible(true);

	    //treePanel.setBounds(r.x, r.y, width, height);
	    //treePanel.setPreferredSize(new Dimension(width+1, height+1));
	    //treeScroll.update(treeScroll.getGraphics());
	    // treePanel.repaint();
	    // treePanel.repaint();
	    // fnScroll.getViewport().add(fnLabel);

	    // Update the UI
	    //
	    paintAll(getGraphics());
	}
    }



    /**
     *  Draw the tree at index DTDindex
     *
     */    
    void drawTree()
    {
	
	// assert
	// ------
	// if( rootElements.size() != xmlTreePanelVect.size() )
	if( rootElementsVect.size() != xmlTreePanelVect.size() )
	{
	    System.err.println("impossible rootElements.size != xmlTreePanelVect.size");
	    return;
	}

	// initialize the tree structure
	// -----------------------------
	// xmlTreeStruVect = new Vector();  // in init();
	Vector dtdElements  = (Vector) (dtdElementsVect.elementAt(DTDindex));
	
	for( int i = DTDindex; i < rootElementsVect.size(); i++ )
	{
	    Vector thisTree = new Vector();
	    xmlTreeStruVect.addElement(thisTree);
	}

	// initialize each panel
	// ---------------------
	for(int i = DTDindex; i < xmlTreePanelVect.size(); i++ )
	{
	    JScrollPane currScroll = (JScrollPane) (xmlTreeScrollVect.elementAt(i));

	    // current panel
	    JPanel currPanel = (JPanel) (xmlTreePanelVect.elementAt(i));

	    // current tree
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));

	    // current root
	    RootElement currRoot = (RootElement) (rootElementsVect.elementAt(i));

	    // current element entry
	    DTDEleEntry currEleEntry = (DTDEleEntry) (dtdElements.elementAt(currRoot.index));

	    // current element name
	    String currEleName = currEleEntry.eleName;

	    // current element's contents
	    Vector currEleContents = currEleEntry.eleContents;

	    // current element's attributes
	    Vector currAttrs = currEleEntry.attrs;


	    int line = 0;

	    
	    // Draw tree ???
	    //
	    {
		JButton openButton = new JButton();
		openButton.setIcon(openIcon);
		openButton.setBackground(tbg);
		openButton.setBorder(emptyBorder);
		
		JButton projButton = new JButton();
		projButton.setIcon(blueIcon);
		projButton.setBorder(emptyBorder);
		projButton.setBackground(tbg);
		
		JLabel fieldLabel = new JLabel(currEleName);
		fieldLabel.setFont(labelFont);
		fieldLabel.setForeground(black);
		openButton.addActionListener(actionLn);
		projButton.addActionListener(actionLn);
		fieldLabel.addMouseListener(mouseLn);
		
		boolean isProjected = false;
		boolean isLeaf = isLeaf(i, currEleName);
		boolean isOpened = true;
		boolean isRemoved = false;
		boolean isAttribute = false;
		int     indent = 0;
		String  path = currEleName;
		
		Entry currEntry = new Entry(
					    openButton, projButton, fieldLabel, 
					    isProjected, isLeaf, isOpened, 
					    isRemoved, isAttribute, path,
					    indent, line);
		line++;
		
		// insert to the currTree
		currTree.addElement(currEntry);
	    }

	    // iterate through element contents
	    String upLevelPath = currEleName;
	    for( int j = 0; j < currEleContents.size(); j++ )
	    {
		String contentStr = (String) (currEleContents.elementAt(j));

		JButton openButton = new JButton();
		//openButton.setIcon(closeIcon);
		openButton.setBackground(tbg);

		JButton projButton = new JButton();
		projButton.setIcon(blueIcon);
		projButton.setBorder(emptyBorder);
		projButton.setBackground(tbg);

		JLabel fieldLabel = new JLabel(contentStr);
		fieldLabel.setFont(labelFont);
		fieldLabel.setForeground(black);
		boolean isProjected = false;
		boolean isLeaf = isLeaf(i, contentStr);
		boolean isOpened = false;
		boolean isRemoved = false;
		boolean isAttribute = false;
		int     indent = 1;
		String  path = upLevelPath +"."+ contentStr;

		if( !isLeaf )
		    openButton.addActionListener(actionLn);
		projButton.addActionListener(actionLn);
		fieldLabel.addMouseListener(mouseLn);

		Entry currEntry = new Entry(
			openButton, projButton, fieldLabel, 
			isProjected, isLeaf, isOpened, 
			isRemoved, 
			isAttribute,
			path,
			indent, line);
		line++;

		// insert to the tree
		currTree.addElement(currEntry);
	    }

	    // iterate through each attributes
	    for( int j = 0; j < currAttrs.size(); j++ )
	    {
		String attrStr = (String) (currAttrs.elementAt(j));

		JButton openButton = new JButton();
		openButton.setIcon(closeIcon);
		openButton.setBackground(tbg);
		openButton.setBorder(emptyBorder);

		JButton projButton = new JButton();
		projButton.setIcon(yellowIcon);
		projButton.setBorder(emptyBorder);
		projButton.setBackground(tbg);

		JLabel fieldLabel = new JLabel(attrStr);
		fieldLabel.setFont(labelFont);
		fieldLabel.setForeground(black);
		boolean isProjected = false;
		boolean isLeaf = true;
		boolean isOpened = false;
		boolean isRemoved = false;
		boolean isAttribute = true;
		int     indent = 1;
		String  path = upLevelPath +"."+ attrStr;

		projButton.addActionListener(actionLn);
		fieldLabel.addMouseListener(mouseLn);

		Entry currEntry = new Entry(
			openButton, projButton, fieldLabel, 
			isProjected, isLeaf, isOpened, 
			isRemoved, 
			isAttribute,
			path,
			indent, line);
		line++;

		// insert to the tree
		currTree.addElement(currEntry);
	    }

	    drawWholeTree();
	}
    }

    /**
     *  Exit with a message
     *
     */    
    void crash(String message){
	System.out.println(message);
	System.exit(1);
    }



    /**
     *  Redraw tree at index panel index.  Begin re-drawing at level startIdx
     *  
     */    
    void redrawTree(int panelIdx, Vector currTree, int startIdx)
    {

	int tot_x=0;
	int tot_y=0; 

	// The tree's scrollPane
	//
	JScrollPane currScroll = (JScrollPane) (xmlTreeScrollVect.elementAt(panelIdx));

	// The tree panel
	//
	JPanel currPanel = (JPanel) (xmlTreePanelVect.elementAt(panelIdx));

	// Entry
	//
	Entry firstEntry = (Entry) (currTree.elementAt(startIdx));

	
	if( firstEntry.isOpened )
	{
	    firstEntry.openButton.setIcon(openIcon);
	}
	else
	{
	    firstEntry.openButton.setIcon(closeIcon);
	}
	firstEntry.openButton.setBorder(emptyBorder);
	firstEntry.openButton.setBackground(tbg);	
	firstEntry.openButton.setVisible(true);

	// iterate through each line in the tree
	//
	for( int i = startIdx+1; i < currTree.size(); i++ )
	{
	    Entry currEntry = (Entry) (currTree.elementAt(i));

	    if( currEntry == null) crash("got a null pointer for curEntry "+i+" in redrawTree()");

	    if( currEntry.isRemoved ) continue;

	    JButton openButton = currEntry.openButton;
	    JButton projButton = currEntry.projButton;
	    JLabel   fieldLabel = currEntry.fieldLabel;
	    Font fieldFont = currEntry.labelFont;
	    boolean isProjected = currEntry.isProjected;
	    boolean isLeaf = currEntry.isLeaf;
	    boolean isOpened = currEntry.isOpened;
	    int     line = currEntry.line;
	    int     indent = currEntry.indent;
	    String predicate = currEntry.predicate;
	    //System.out.println("pred: "+predicate);

	    // not leaf, draw the open-close icon
	    int x = (indent+1)*IndentWidth;
	    int y = (line+1)*LineHeight;
	    tot_x = x;
	    tot_y = y;
	    
	    // set proper icon
	    if( !isLeaf )
	    {
		if( isOpened )
		{
		    openButton.setIcon(openIcon);
		}
		else
		{
		    openButton.setIcon(closeIcon);
		}

		openButton.setBounds(x, y, OpenButtonWidth, OpenButtonHeight);
		openButton.setBorder(emptyBorder);
		openButton.setBackground(tbg);	
		openButton.setVisible(true);
	    }
	    
	    // project ball
	    //
	    x = x+OpenButtonWidth+HorizonDist;
	    tot_x = x;
	    projButton.setBounds(x, y+2, ProjButtonRadius, ProjButtonRadius);
	    projButton.setBorder(emptyBorder);
	    projButton.setVisible(true);

	    // label
	    //
	    x = x+ProjButtonRadius+HorizonDist;
	    tot_x = x+LabelWidth;
	    fieldLabel.setBounds(x,y, LabelWidth, LabelHeight);
	    fieldLabel.setForeground(currEntry.joinColor);
	    fieldLabel.setFont(fieldFont);

	    //if(predicate != null && predicate.trim().length() != 0)
	    //else
	    //fieldLabel.setFont(labelFont);

	    fieldLabel.setVisible(true);
	}


	// Re-set the bounds so the scrollbars are reset
	//
	Rectangle r = currPanel.getBounds();
	int width = r.width;
	int height = r.height;
	//Dimension d = currPanel.getSize();
	//d.height+=2;
	//d.width+=2;
	//currPanel.setSize(d);

	//System.out.println("------------------\ncurrBounds: "+r.toString());
	//System.out.println("max size of this panel: "+currPanel.getMaximumSize().toString());
	//System.out.println("min size of this panel: "+currPanel.getMinimumSize().toString());
	//System.out.println("pref size of this panel: "+currPanel.getPreferredSize().toString());
	//System.out.println("------------------");

	if(width < tot_x)
	    width = tot_x;
	if(height < tot_y)
	    height = tot_y;

	//System.out.println("x="+r.x+", y="+r.y+", width="+(width+10)+", height="+(height+20));

	currPanel.doLayout();
	currPanel.setBounds(0,0, width+10, height+20);
	currPanel.setPreferredSize(new Dimension(width+10, height+20));
    }



    /**
     *  Draw the whole tree for trees at indexes DTDindex and higher
     *
     */    
    void drawWholeTree()
    {

	//System.out.println("drawWholeTree() called");

	// iterate through each tree
	for( int i = DTDindex; i < xmlTreeStruVect.size(); i++ )
	{
	    
	    // curr scroll
	    //
	    JScrollPane currScroll = (JScrollPane) (xmlTreeScrollVect.elementAt(i));

	    // current panel
	    JPanel currPanel = (JPanel) (xmlTreePanelVect.elementAt(i));

	    // current tree
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));

	    // iterate through each line in the tree
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry currEntry = (Entry) (currTree.elementAt(j));

		if( currEntry.isRemoved ) continue;

		JButton openButton = currEntry.openButton;
		JButton projButton = currEntry.projButton;
		JLabel   fieldLabel = currEntry.fieldLabel;
		boolean isProjected = currEntry.isProjected;
		boolean isLeaf = currEntry.isLeaf;
		boolean isOpened = currEntry.isOpened;
		int     line = currEntry.line;
		int     indent = currEntry.indent;

		// set proper icon
		if( isOpened )
		    openButton.setIcon(openIcon);
		else
		    openButton.setIcon(closeIcon);
		openButton.setBackground(tbg);	
		openButton.setBorder(emptyBorder);

		// not leaf, draw the open-close icon
		int x = (indent+1)*IndentWidth;
		int y = (line+1)*LineHeight;
		if( !isLeaf )
		{
		    openButton.setBounds(x, y, OpenButtonWidth, OpenButtonHeight);
		    currPanel.add(openButton);
		}
		
		// project
		x = x+OpenButtonWidth+HorizonDist;
		projButton.setBounds(x, y+2, ProjButtonRadius, ProjButtonRadius);
		currPanel.add(projButton);

		//label
		x = x+ProjButtonRadius+HorizonDist;
		fieldLabel.setBounds(x,y, LabelWidth, LabelHeight);
		currPanel.add(fieldLabel);
	    }
	}
	paintAll(getGraphics());
    }


    /**
     * get root leaf attr  ???
     *
     */    
    int getRoot_Leaf_Attr()
    {

	int    rootNum = 0;

	Vector dtdElements = (Vector) (dtdElementsVect.elementAt(DTDindex));
	for( int i = 0; i < dtdElements.size(); i++ )
	{
	    DTDEleEntry currEntry = (DTDEleEntry) (dtdElements.elementAt(i));
	    String eleName = currEntry.eleName;

	    boolean found = false;
	    for( int j = 0; j < dtdElements.size(); j++ )
	    {
		DTDEleEntry subEntry = (DTDEleEntry) (dtdElements.elementAt(j));
		Vector contents = subEntry.eleContents;
		for( int k = 0; k < contents.size(); k++ )
		{
		    String currCont = (String) (contents.elementAt(k));
		    
		    if( currCont.equals( eleName ) )
		    {
			found = true;
			break;
		    }
		}
		if( found ) break;
	    }
	    if( !found )
	    {
		RootElement newRootEle = new RootElement(eleName, i);
		rootElementsVect.addElement(newRootEle);
		rootNum++;
	    }
	}

	// get leaf attr list
	// ------------------
	Vector leafContents = new Vector();
	Vector attrList = new Vector();

	//Vector dtdElements = (Vector) (dtdElementsVect.elementAt(DTDindex));
	for( int i = 0; i < dtdElements.size(); i++ )
	{
	    DTDEleEntry eleEntry = (DTDEleEntry) (dtdElements.elementAt(i));
	    Vector attrs = eleEntry.attrs;
	    Vector eleContents = eleEntry.eleContents;

	    // add to attrList
	    for( int j = 0; j < attrs.size(); j++ )
	    {
		String attrStr = (String) (attrs.elementAt(j));
		attrList.addElement(attrStr);
	    }

	    // add to leafContents
	    for( int j = 0; j < eleContents.size(); j++ )
	    {
		String eleStr = (String) (eleContents.elementAt(j));
		// search the elements
		int k;
		boolean isTheLeaf = false;
		for( k = 0; k < dtdElements.size(); k++ )
		{
		    DTDEleEntry currEntry = (DTDEleEntry) (dtdElements.elementAt(k));
		    if( eleStr.equals( currEntry.eleName ) ) 
		    {
			if( currEntry.eleContents.size() == 0 && currEntry.attrs.size() == 0)
			    isTheLeaf = true;
			break;
		    }
		}
		//if( k == dtdElements.size() )
		if( k == dtdElements.size() || isTheLeaf )
		{
		    int l ;
		    for( l = 0; l < leafContents.size(); l++ )
		    {
			String currLeaf = (String) (leafContents.elementAt(l));
			//if( currLeaf == eleStr ) break; on May.6
			if( currLeaf.equals( eleStr ) ) break;
		    }
		    if( l == leafContents.size() )
		    {
			leafContents.addElement(eleStr);
		    }
		}
	    }
	}

	// if there are multiple roots, each should have a leafContents vector and attrList vector
	// actually, there is only one copy of each list if they are same.
	for( int i = 0; i < rootNum; i++ )
	{
	    leafContentsVect.addElement(leafContents);
	    attrListVect.addElement(attrList); 
	}

	return rootNum;
    }


    /**
     *  Get the root elements for DTD at index DTDindex
     *
     */    
    void getRootElements()
    {
	Vector dtdElements = (Vector) (dtdElementsVect.elementAt(DTDindex));
	for( int i = 0; i < dtdElements.size(); i++ )
	{
	    DTDEleEntry currEntry = (DTDEleEntry) (dtdElements.elementAt(i));
	    String eleName = currEntry.eleName;

	    boolean found = false;
	    for( int j = 0; j < dtdElements.size(); j++ )
	    {
		DTDEleEntry subEntry = (DTDEleEntry) (dtdElements.elementAt(j));
		Vector contents = subEntry.eleContents;
		for( int k = 0; k < contents.size(); k++ )
		{
		    String currCont = (String) (contents.elementAt(k));
		    if( currCont == eleName )
		    {
			found = true;
			break;
		    }
		}
		if( found ) break;
	    }
	    if( !found )
	    {
		RootElement newRootEle = new RootElement(eleName, i);
		rootElementsVect.addElement(newRootEle);
		DTDindex ++;
	    }
	}
    }



    /**
     *  get the leaf nodes
     *
     */    
    void getLeafAttr()
    {
	Vector leafContents = new Vector();
	Vector attrList = new Vector();

	Vector dtdElements = (Vector) (dtdElementsVect.elementAt(DTDindex));
	for( int i = 0; i < dtdElements.size(); i++ )
	{
	    DTDEleEntry eleEntry = (DTDEleEntry) (dtdElements.elementAt(i));
	    Vector attrs = eleEntry.attrs;
	    Vector eleContents = eleEntry.eleContents;

	    // add to attrList
	    for( int j = 0; j < attrs.size(); j++ )
	    {
		String attrStr = (String) (attrs.elementAt(j));
		attrList.addElement(attrStr);
	    }

	    // add to leafContents
	    for( int j = 0; j < eleContents.size(); j++ )
	    {
		String eleStr = (String) (eleContents.elementAt(j));
		// search the elements
		int k;
		for( k = 0; k < dtdElements.size(); k++ )
		{
		    DTDEleEntry currEntry = (DTDEleEntry) (dtdElements.elementAt(k));
		    if( eleStr == currEntry.eleName ) break;
		}
		if( k == dtdElements.size() )
		{
		    int l ;
		    for( l = 0; l < leafContents.size(); l++ )
		    {
			String currLeaf = (String) (leafContents.elementAt(l));
			if( currLeaf == eleStr ) break;
		    }
		    if( l == leafContents.size() )
		    {
			leafContents.addElement(eleStr);
		    }
		}
	    }
	}

	leafContentsVect.addElement(leafContents);
	attrListVect.addElement(attrList); 
    }


    /**
     *  Add an event listener to various gui components.  Create a mouse listener
     *
     */    
    void addEventListener()
    {
	actionLn = new ActionLn();
	loadButton.addActionListener(actionLn);
	resetButton.addActionListener(actionLn);
	showButton.addActionListener(actionLn);
	goExitButton.addActionListener(actionLn);

	joinStButton.addActionListener(actionLn);
	joinEndButton.addActionListener(actionLn);
	joinCancelButton.addActionListener(actionLn);

	mouseLn = new SymMouse();
    }

    /**
     *  This class is our own Action Listener
     *
     */    
    class ActionLn implements ActionListener
    {
	public void actionPerformed(ActionEvent event)
	{
	    Object object = event.getSource();

	    // load button
	    //
	    if( object == loadButton)
	    {
		loadButton_action();
	    }

	    // reset button
	    //
	    else if( object == resetButton )
	    {
		resetButton_action();
	    }
	    
	    // go and exit button
	    //
	    else if( object == goExitButton )
	    {
		goExitButton_action();
	    }

	    // show button
	    else if( object == showButton )
	    {
		showButton_action();
	    }
	    
	    // Join start
	    //
	    else if( object == joinStButton)
	    {
		joinSt_action();
	    }
	    
	    // Join end
	    //
	    else if( object == joinEndButton )
	    {
		joinEnd_action();
	    }
	    
	    // Join cancel
	    //
	    else if( object == joinCancelButton )
	    {
		joinCancel_action();
	    }

	    // buttons hit in tree
	    //
	    else
	    {
		drawButton_action(object);
	    }
	}
    }


    /**
     *  Process a join start action
     *
     */    
    void joinSt_action()
    {
	joinState = true;
	curJoinColor = joinColor.nextColor();
	joinStButton.setForeground(Color.red);
    }


    /**
     *  Process a join end action
     *
     */    
    void joinEnd_action()
    {
	joinState = false;
	joinStButton.setForeground(Color.black);

	Vector joins = tempJoins;
	tempJoins = new Vector();
	
	// check whether some contents have been joined before 
	boolean alreadyJoined = false;
	int     oldJoinIndex = 0;      // 0 = not joined
	Entry   oldEntry = null;
	for( int i = 0 ; i < joins.size(); i++ )
	{
	    Entry joinEntry = (Entry) ( ((JoinInput)(joins.elementAt(i))).entry);

	    for( int j = 0; j < joinVect.size(); j++ )
	    {
		Vector currJoins = (Vector) (joinVect.elementAt(j));

		for( int k = 0; k < currJoins.size(); k++ )
		{
		    Entry tmpEntry = (Entry) (((JoinInput)(currJoins.elementAt(k))).entry);

		    if( tmpEntry.fieldLabel == joinEntry.fieldLabel )
		    {
			oldJoinIndex = tmpEntry.joinIndex;
			oldEntry = tmpEntry;
			alreadyJoined = true;
			break;
		    }
		}
		if( alreadyJoined ) break;
	    }
	    if( alreadyJoined ) break;
	}

	// already joined, change the join index to be the old one
	//
	if( alreadyJoined )
	{
	    System.out.println(" already joined ");
	    for( int i = 0; i < joins.size(); i++ )
	    {
		Entry joinEntry = (Entry) (((JoinInput)(joins.elementAt(i))).entry);
		joinEntry.joinIndex = oldJoinIndex;
		joinEntry.fieldLabel.setForeground(oldEntry.joinColor);
	    }

	}
	else
	{
	    // increase join number
	    joinSeqNum++;

	    //Color tmpColor = joinColor.nextColor();
	    for( int i = 0; i < joins.size(); i++ )
	    {
		Entry joinEntry = (Entry) (((JoinInput)(joins.elementAt(i))).entry);
		joinEntry.fieldLabel.setForeground(curJoinColor);
		joinEntry.joinColor = curJoinColor;
	    }
	}

	// add the joins to the joinVect
	joinVect.addElement(joins);
    }



    /**
     *  Process a join cancel action
     *
     */    
    void joinCancel_action()
    {
	joinState = false;
	joinStButton.setForeground(Color.black);
    }



    /**
     *  Process a clear tree action
     *
     */    
    void clearTree()
    {
	if( xmlQlFrame != null )
	    xmlQlFrame.xmlQlDialog.setQL( "" );

	// iterate through each tree
	// -------------------------
	for( int i = 0; i < xmlTreeStruVect.size(); i++ )
	{
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));
	    
	    // iterate through each entry in the tree
	    // --------------------------------------
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry currEntry = (Entry) (currTree.elementAt(j));

		if( currEntry.isLeaf )
		    currEntry.projButton.setIcon(yellowIcon); 
		else
		    currEntry.projButton.setIcon(blueIcon); 
		currEntry.isChosen = false;
		currEntry.isProjected = false;
		currEntry.isConstructed = false;
		currEntry.predicate = null;
		currEntry.joinIndex = 0;
		currEntry.joinColor = Color.black;
	    }

	    // redraw the tree
	    // ---------------
	    redrawTree(i, currTree, 0);
	}
    }



    /**
     *  Process a reset button action
     *
     */    
    public void resetButton_action()
    {
	if( xmlQlFrame != null )
	    xmlQlFrame.xmlQlDialog.setQL( "" );

	// iterate through each tree
	// -------------------------
	for( int i = 0; i < xmlTreeStruVect.size(); i++ )
	{
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));
	    
	    // iterate through each entry in the tree
	    // --------------------------------------
	    int lineNum = 0;
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry currEntry = (Entry) (currTree.elementAt(j));

		// opened or not
		if( j == 0 )
		{
		    currEntry.isOpened = true;
		}
		else
		{
		    currEntry.isOpened = false;
		}

		// line number in the tree
		currEntry.line = lineNum;

		// removed or not
		// indent 0, 1, not removed
		if( currEntry.indent == 0 || currEntry.indent == 1 )
		{
		    currEntry.isRemoved = false;
		    lineNum++;
		}
		else
		{
		    currEntry.isRemoved = true;
		    if( ! currEntry.isLeaf )
			currEntry.openButton.setVisible(false);
		    currEntry.projButton.setVisible(false);
		    currEntry.fieldLabel.setVisible(false);
		}
		
		if( currEntry.isAttribute )
		    currEntry.projButton.setIcon(yellowIcon); 
		else
		    currEntry.projButton.setIcon(blueIcon); 
		currEntry.isChosen = false;
		currEntry.isProjected = false;
		currEntry.isConstructed = false;
		currEntry.predicate = null;
		currEntry.setLabel(currEntry.fieldName);
		currEntry.setLabelFont(labelFont);
		currEntry.joinIndex = 0;
		currEntry.joinColor = Color.black;

	    }

	    // redraw the tree
	    // ---------------
	    redrawTree(i, currTree, 0);
	}
    }


    /**
     *  Process a go button click action
     *
     */    
    public void goButton_action()
    {
	// build inputToParser( vector of Input )
	// --------------------------------------
	Vector inputToParser = new Vector();
	Input  currInput;
	
	// iterate each tree
	for( int i = 0; i < xmlTreeStruVect.size(); i++)
	{
	    Vector inputs = new Vector();

	    // DTD Names are stored here
	    //
	    String inWhich = (String) (dtdNames.elementAt(i));

	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));

	    // iterate through entries in the tree
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry tmpEntry = (Entry) (currTree.elementAt(j));
		if( tmpEntry.isChosen )
		{
		    // clean the white spaces of predicates
		    if( tmpEntry.predicate != null )
		    {
			tmpEntry.predicate = tmpEntry.predicate.trim();
			if( tmpEntry.predicate.length() == 0 )
			    tmpEntry.predicate = null;
		    }

		    inputs.addElement(tmpEntry);
		}
	    }

	    currInput = new Input(inWhich, inputs);
	    inputToParser.addElement(currInput);
	}

	// send the inputToPaser and joinVect to parser
	// ---------------------
	MyQuery q = new MyQuery();
	q.genQuery(inputToParser);

	// show query-ql
	System.out.println(q.query);
	xmlQlStr = q.query;
    }



    /**
     *  Process a goExit button click action
     *
     */    
    public void goExitButton_action()
    {
	goButton_action();

	//
	resultBuffer.append(xmlQlStr);
	    
	//Gui.qbeResultString = xmlQlStr;
	this.setVisible(false);
    }


    /**
     *  Process a show query button click action
     *
     */    
    public void showButton_action()
    {
	goButton_action();
	xmlQlFrame = new XmlQlFrame(new String("XML Query"), xmlQlStr);
	//xmlQlFrame.xmlQlDialog.setQL(xmlQlStr);
    }



    /**
     *  Initialize the XmlInterface Dialog frame
     *
     */    
    public void createFrame()
    {

	this.getContentPane().setLayout(null);
	this.setBackground(bg);

	// panel of each part on the main panel
	// ------------
	buttonPanel = new JPanel();
	joinPanel   = new JPanel();

	int startHeight = 0;

	// button panel
	// ------------
	this.getContentPane().add(buttonPanel);
	buttonPanel.setLayout(null);
	buttonPanel.setBounds(0, 0+startHeight, InterfaceWidth, ButtonPaneHt);
	buttonPanel.setBackground(bg);

	{
	    loadButton = new JButton("Load DTD");
	    loadButton.setFont(buttonFont);
	    buttonPanel.add(loadButton);
	    int buttPosx = 70;// 110
	    int buttPosy = 10;
	    int buttDist = 45;//60
	    loadButton.setBounds(buttPosx, buttPosy, ButtonWidth, ButtonHeight);
	    loadButton.setBackground(buttonColor);
	    
	    resetButton = new JButton("Reset");
	    resetButton.setFont(buttonFont);
	    buttonPanel.add(resetButton);
	    buttPosx += (ButtonWidth+buttDist);
	    resetButton.setBounds(buttPosx, buttPosy, ButtonWidth, ButtonHeight);
	    resetButton.setBackground(buttonColor);
	    
	    showButton = new JButton("Show XML-QL");
	    buttonPanel.add(showButton);
	    buttPosx += (ButtonWidth+buttDist);
	    showButton.setFont(buttonFont);
	    showButton.setBounds(buttPosx, buttPosy, ButtonWidth, ButtonHeight);
	    showButton.setBackground(buttonColor);
	    
	    goExitButton = new JButton("Done");
	    goExitButton.setFont(buttonFont);
	    buttonPanel.add(goExitButton);
	    buttPosx += (ButtonWidth+buttDist);
	    goExitButton.setBounds(buttPosx, buttPosy, ButtonWidth, ButtonHeight);
	    goExitButton.setBackground(buttonColor);
	}
	
	
	// treePanel
	//
	treePanel = new JPanel();
	treePanel.setLayout(null);
	treePanel.setBackground(bg);
	treePanel.setPreferredSize(new Dimension(3000, 600));

	//treePanel.setMaximumSize(null);

	treePanel.setBounds(0, 0, MAX_PANEL_WIDTH, MAX_PANEL_HEIGHT);


	// Tree scrollpane with treePanel as the Scrollable client
	// 
	treeScroll  = new JScrollPane(treePanel, JScrollPane.VERTICAL_SCROLLBAR_NEVER,
				      JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
	treeScroll.setBounds(0, startHeight+ButtonPaneHt+2, 
			     InterfaceWidth, TreePaneHt);
	//treeScroll.setLayout(new ScrollPaneLayout());
	treeScroll.setBackground(Color.white);
	treeScroll.setVisible(true);
	this.getContentPane().add(treeScroll);


	
	// join panel 
	// ------------
	this.getContentPane().add(joinPanel);
	joinPanel.setLayout(null);
	joinPanel.setBounds(0, startHeight+ButtonPaneHt+TreePaneHt+4, 
			    InterfaceWidth, JoinPaneHt);
	joinPanel.setBackground(bg);

	joinStButton = new JButton("Start Join");
	joinEndButton = new JButton("End Join");
	joinCancelButton = new JButton("Cancel Join");
	
	joinStButton.setBounds(110,15, ButtonWidth, ButtonHeight);// 150 15
	joinEndButton.setBounds(300,15, ButtonWidth, ButtonHeight);
	joinCancelButton.setBounds(470, 15, ButtonWidth, ButtonHeight);

	joinStButton.setFont(buttonFont);
	joinEndButton.setFont(buttonFont);
	joinCancelButton.setFont(buttonFont);
	
	joinStButton.setBackground(buttonColor);
	joinEndButton.setBackground(buttonColor);
	joinCancelButton.setBackground(buttonColor);
	
	joinPanel.add(joinStButton);
	joinPanel.add(joinEndButton);
	joinPanel.add(joinCancelButton);
    }

    /**
     *  Determine if a given element mname is a leaf element
     *
     */    
    boolean isLeaf(int treeIndex, String leafName)
    {
	int i;
	Vector leafContents = (Vector) (leafContentsVect.elementAt(treeIndex));
	for( i = 0; i < leafContents.size(); i++ )
	{
	    String currLeaf = (String) (leafContents.elementAt(i));
	    if( currLeaf == leafName )
		break;
	}
	if( i == leafContents.size() )
	    return false;
	else 
	    return true;
    }


    /**
     *  Determine if a node name is an attribute
     *
     */    
    boolean isAttr(int treeIndex, String attrName)
    {
	int i;
	Vector attrList = (Vector) (attrListVect.elementAt(treeIndex));
	for( i = 0; i < attrList.size(); i++ )
	{
	    String currAttr = (String) (attrList.elementAt(i));
	    if( currAttr == attrName )
		break;
	}
	if( i == attrList.size() )
	    return false;
	else
	    return true;
    }



    /**
     *  Called when an event is created as a result of clicking on a DTD
     *  tree node
     *
     */    
    void drawButton_action(Object object)
    {
	// find the object in xmlTreeStruVect
	// ----------------------------------
	ObjectDes objectDes =  findObject(object);

	Vector currTree = objectDes.tree;
	Entry currEntry = objectDes.entry;
	int startIndex = objectDes.startIndex;
	int panelIndex = objectDes.panelIndex;

	int whichButton = objectDes.whichButton;

	// open button, insert or remove 
	// -----------------------------
	if( whichButton == OPEN_BUTTON )
	{
	    // not leaf
	    // --------
	    if( !(currEntry.isLeaf) )
	    {
		// going to be closed
		// ------------------
		if( currEntry.isOpened )
		{
		    // mark delete 
		    // -----------
		    int currIndex = startIndex;
		    currEntry.isOpened = false;

		    int removeNum = 0;
		    int indent = currEntry.indent+1;
		    currIndex = startIndex+1;

		    while( currIndex < currTree.size() )
		    {
			Entry tmpEntry = (Entry) (currTree.elementAt(currIndex));
			if( tmpEntry.indent >= indent )
			{
			    removeNum++;
			    tmpEntry.isOpened = false;
			    tmpEntry.isRemoved = true;

			    // set buttons invisible
			    if( !tmpEntry.isLeaf )
				tmpEntry.openButton.setVisible(false);
			    tmpEntry.projButton.setVisible(false);
			    tmpEntry.fieldLabel.setVisible(false);
			    currIndex++;
			}
			else
			    break;
		    }
		    
		    // following line decreased
		    // -------------------------
		    for( int i = currIndex; i < currTree.size(); i++ )
		    {
			Entry tmpEntry = (Entry) (currTree.elementAt(i));
			tmpEntry.line -= removeNum;
		    }

		    // redraw the tree
		    // ---------------
		    redrawTree(panelIndex, currTree, 0);
		}

		// OPEN BUTTON, going to be opened
		// -------------------------------
		else
		{

		    currEntry.isOpened = true;

		    // whether the currTree has the entry for it or not
		    boolean inserted = false;
		    if( startIndex < ( currTree.size() - 1 ) )
		    {
			int nextIndent = ( (Entry) (currTree.elementAt(startIndex+1)) ).indent;
			if( currEntry.indent == (nextIndent - 1) )
			    inserted = true;
		    }

		    //System.out.println("inserted already? "+inserted);

		    int insertNum = 0;
		    // expand to next level
		    if( !inserted && !currEntry.isLeaf )
		    {
			String tmpName = currEntry.fieldLabel.getText();
			String upLevelPath = currEntry.path;
			Vector tmpContents;
			Vector tmpAttrs;
			Vector dtdElements = (Vector) (dtdElementsVect.elementAt(panelIndex));
			for( int i = 0; i < dtdElements.size(); i++ )
			{
			    DTDEleEntry tmpEleEntry = (DTDEleEntry) (dtdElements.elementAt(i));
			    // find the element
			    if( tmpName == tmpEleEntry.eleName)
			    {
				tmpContents = tmpEleEntry.eleContents;
				tmpAttrs = tmpEleEntry.attrs;

				JPanel currPanel = (JPanel) (xmlTreePanelVect.elementAt(panelIndex));

				// insert the element contents to tree
				for( int j = 0; j < tmpContents.size(); j++ )
				{

				    String tmpFieldName = (String) (tmpContents.elementAt(j));

				    JButton openButton = new JButton();
				    openButton.setIcon(closeIcon);
				    openButton.setBorder(emptyBorder);
				    openButton.setBackground(tbg);

				    JButton projButton = new JButton();
				    projButton.setIcon(blueIcon);
				    projButton.setBorder(emptyBorder);
				    projButton.setBackground(tbg);

				    JLabel fieldLabel = new JLabel(tmpFieldName);
				    fieldLabel.setFont(labelFont);
				    fieldLabel.setForeground(black);
				    openButton.addActionListener(actionLn);
				    projButton.addActionListener(actionLn);
				    fieldLabel.addMouseListener(mouseLn);

				    boolean isProjected = false;
				    boolean isOpened = false;
				    boolean isLeaf = isLeaf(panelIndex, tmpFieldName);
				    boolean isRemoved = false;
				    boolean isAttribute = false;
				    insertNum++;
				    int     indent = currEntry.indent + 1;
				    int     line = currEntry.line + insertNum;
				    String  path = upLevelPath +"."+ tmpFieldName;

				    if( !isLeaf )
					currPanel.add(openButton);
				    currPanel.add(projButton);
				    currPanel.add(fieldLabel);

				    Entry newEntry = new Entry(
						openButton, projButton, fieldLabel, 
						isProjected, isLeaf, isOpened, 
						isRemoved, 
						isAttribute,
						path,
						indent, line);

				    currTree.insertElementAt(newEntry, startIndex+insertNum);
				}

				// insert the element attributes to tree
				for( int j = 0; j < tmpAttrs.size(); j++ )
				{
				    String tmpFieldName = (String) (tmpAttrs.elementAt(j));

				    JButton openButton = new JButton();
				    openButton.setIcon(closeIcon);
				    openButton.setBorder(emptyBorder);
				    openButton.setBackground(tbg);

				    JButton projButton = new JButton();
				    projButton.setIcon(yellowIcon);
				    projButton.setBorder(emptyBorder);
				    projButton.setBackground(tbg);

				    JLabel fieldLabel = new JLabel(tmpFieldName);
				    fieldLabel.setFont(labelFont);
				    fieldLabel.setForeground(black);
				    openButton.addActionListener(actionLn);
				    projButton.addActionListener(actionLn);
				    fieldLabel.addMouseListener(mouseLn);

				    boolean isProjected = false;
				    boolean isLeaf = true;
				    boolean isOpened = false;
				    boolean isRemoved = false;
				    boolean isAttribute = true;
				    insertNum++;
				    int     indent = currEntry.indent + 1;
				    int     line = currEntry.line + insertNum;
				    String  path = upLevelPath +"."+ tmpFieldName;

				    if( !isLeaf )
					currPanel.add(openButton);
				    currPanel.add(projButton);
				    currPanel.add(fieldLabel);

				    Entry newEntry = new Entry(
						openButton, projButton, fieldLabel, 
						isProjected, isLeaf, isOpened, 
						isRemoved, 
						isAttribute,
						path,
						indent, line);

				    currTree.insertElementAt(newEntry, startIndex+insertNum);
				}

				// adjust following entries in the tree
				for( int j = (startIndex+insertNum+1); j < currTree.size(); j++ )
				{
				    Entry tmpEntry = (Entry) (currTree.elementAt(j));
				    tmpEntry.line += insertNum;
				}

				break;
			    }
			}
		    }

		    // reset 
		    else
		    {
			int resetNum = 0;
			int indent = currEntry.indent+1;
			int currIndex = startIndex+1;
			Entry tmpEntry = (Entry) (currTree.elementAt(currIndex));
			while( tmpEntry.indent >= indent )
			{
			    
			    if( tmpEntry.isRemoved && tmpEntry.indent == indent )
			    {
				resetNum++;
				tmpEntry.isRemoved = false;
				tmpEntry.line = currEntry.line+resetNum;
				if( !tmpEntry.isLeaf )
				    tmpEntry.isOpened = false;
			    }
			    currIndex++;

			    if( currIndex >= currTree.size() ) break;

			    tmpEntry = (Entry) (currTree.elementAt(currIndex));
			}
			       
			// adjust following entries in the tree
			for( int i = currIndex; i < currTree.size(); i++ )
			{
			    tmpEntry = (Entry) (currTree.elementAt(i));
			    tmpEntry.line += resetNum;
			}
		    }
		    
		    // redraw the tree
		    // ---------------
		    redrawTree(panelIndex, currTree, 0);  //startIndex    
		}
	    }
	}

	// PROJ BUTTON, dialog, change color or not
	// ----------------------------------------
	else if( whichButton == PROJ_BUTTON )
	{
	    projButton_action_performed(objectDes);
	}
    }


    /**
     *  If an open/close tree node button is clicked on, this function is called
     *
     */    
    void projButton_action_performed(ObjectDes objectDes)
    {
	Vector currTree   = objectDes.tree;
	Entry  currEntry  = objectDes.entry;
	int    panelIndex = objectDes.panelIndex;

	QueryFrame queryFrame = new QueryFrame(new String("XML Query"), objectDes);

	String  predicate   = queryFrame.queryDialog.getPredicate();
	boolean projected   = queryFrame.queryDialog.isProjected();
	boolean constructed = queryFrame.queryDialog.isConstructed();

	// Clicked OK in query dialog
	//
	if( queryFrame.queryDialog.isOkClicked() )
	{
	    currEntry.isChosen = true;
	    currEntry.isProjected = projected;
	    currEntry.isConstructed = constructed;
	    currEntry.predicate = predicate;
	    
	    if(predicate != null){
		currEntry.setLabelFont(predLabelFont);
		currEntry.setLabel(new String(currEntry.fieldName+" ("+predicate+")"));
	    }
	    else{
		currEntry.setLabelFont(labelFont);
		currEntry.setLabel(new String(currEntry.fieldName));
	    }
	}

	// set the projected color
	//	
	if( projected )
	{
	    JButton tmpButton = currEntry.projButton;
	    tmpButton.setIcon(redIcon);
	    tmpButton.setBorder(emptyBorder);
	}
	else
	{
	    JButton tmpButton = currEntry.projButton;
	    if( currEntry.isAttribute )
		tmpButton.setIcon(yellowIcon);
	    else
		tmpButton.setIcon(blueIcon);
 	}
	
	// redraw the tree
	// 
	redrawTree(panelIndex, currTree, 0);
	
    }



    /**
     *  ???
     *
     */    
    ObjectDes findLabelObject(Object object) 
    {
	ObjectDes objectDes = new ObjectDes();

	boolean found = false;
	for( int i = 0; i < xmlTreeStruVect.size(); i++ )
	{
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry currEntry = (Entry) (currTree.elementAt(j));
		
		if( currEntry.fieldLabel == object )
		{
		    found = true;
		    objectDes.entry = currEntry;
		    objectDes.tree = currTree;
		    objectDes.panelIndex = i;
		    break;
		}

	    }
	    if( found ) break;
	}
	return objectDes;
    }



    /**
     *  Called by the drawButton_action() function
     *
     */    
    private ObjectDes findObject(Object object) 
    {
	ObjectDes objectDes = new ObjectDes();

	boolean found = false;
	for( int i = 0; i < xmlTreeStruVect.size(); i++ )
	{
	    Vector currTree = (Vector) (xmlTreeStruVect.elementAt(i));
	    for( int j = 0; j < currTree.size(); j++ )
	    {
		Entry currEntry = (Entry) (currTree.elementAt(j));

		// open button
		//
		if( currEntry.openButton == object )
		{
		    found = true;
		    objectDes.entry = currEntry;
		    objectDes.tree = currTree;
		    objectDes.startIndex = j;
		    objectDes.whichButton = OPEN_BUTTON;
		    objectDes.panelIndex = i;
		    break;
		}

		// proj button
		//
		if( currEntry.projButton == object )
		{
		    found = true;
		    objectDes.entry = currEntry;
		    objectDes.tree = currTree;
		    objectDes.startIndex = j;
		    objectDes.whichButton = PROJ_BUTTON;
		    objectDes.panelIndex = i;
		    break;
		}
	    }
	    if( found ) break;
	}
	return objectDes;
    }


    /**
     *  Print the tree in tecxt format to stdout for debugging
     *
     *  @param thisTree the tree to print
     */    
    private void printTree(Vector thisTree)
    {
	for( int i = 0; i < thisTree.size(); i++ )
	{
	    Entry currEntry = (Entry) (thisTree.elementAt(i));

	    System.out.println("Entry "+currEntry.fieldLabel.getText()+"  path "+currEntry.path);
	    System.out.print("   isProjected "+currEntry.isProjected);
	    System.out.print("   isLeaf "+currEntry.isLeaf);
	    System.out.println("   isOpened "+currEntry.isOpened);
	    System.out.print("   isRemoved "+currEntry.isRemoved);
	    System.out.print("   line "+currEntry.line);
	    System.out.print("   indent "+currEntry.indent);
	    System.out.println();
	    System.out.print("   isConstructed "+currEntry.isConstructed);
	    System.out.println("   isChosen "+currEntry.isChosen);
	    System.out.println("   predicate "+currEntry.predicate);
	}
	System.out.println("----------------------\n");
    }


    /**
     *  Handle a mouse pressed event (received from the mouse listener)
     *
     *  @param object the event triggered by the mouse action
     */    
    void mousePress_action(Object object)
    {
	ObjectDes objectDes = findLabelObject(object);

	JLabel fieldLabel = objectDes.entry.fieldLabel;
	fieldLabel.setFont(labelFont);

	if( joinState)
	{
	    objectDes.entry.joinIndex = joinSeqNum;
	    fieldLabel.setForeground(curJoinColor);
	    objectDes.entry.isChosen = true;

	    String tmpXmlFn = (String) (dtdNames.elementAt(objectDes.panelIndex));
	    JoinInput  tmpJoinEntry = new JoinInput( tmpXmlFn, objectDes.entry);
	    tempJoins.addElement(tmpJoinEntry);
	    // check the joined vector
	}
    }


    /**
     *  Class SymMouse is a mouse event listener
     *
     */    
    class SymMouse extends MouseAdapter
    {
	public void mousePressed(MouseEvent event)
	{
	    Object object = event.getSource();

	    // Mouse pressed, check the join state
	    //
	    mousePress_action(object);
	}
	public void mouseEntered(MouseEvent event) { }
	public void mouseReleased(MouseEvent event) { }
    }
}






