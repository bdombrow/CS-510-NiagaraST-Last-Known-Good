
/**********************************************************************
  $Id: NiagaraUI.java,v 1.2 2000/06/12 01:13:01 vpapad Exp $


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


/**
 *  User interface for the XML Query-Search engine
 *  FileName: NiagaraUI.java
 *  Usage: Javarun client.NiagaraUI <server host name> <server port no>
 */

package niagara.client; 

import java.awt.*;
import java.awt.event.*;
import java.awt.Toolkit;
import java.io.*;
import java.util.Vector;
import java.util.Date;
import java.net.URL;
import java.applet.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.tree.*;

// Have to package XmlInterface classes
import niagara.client.xmlqbe.*;

/** 
 * Main class of the UI
 */
public class NiagaraUI extends JFrame implements ActionListener, ChangeListener, DocumentListener, UIDriverIF {

    // Container for frame
    private Container 		contentPane;
    
    // Panels in the frame
    private JPanel      	queryPanel;
    private JPanel      	resultPanel;
    private JPanel      	statusPanel;
    private JPanel              bPanel;
    private JPanel              iPanel;
    private JPanel              tPanel;
    
    // Menu
    private JMenu       	fileMenu;
    private JMenuBar    	menuBar;
    
    // Text areas
    private JTextArea   	queryText;
    private JTextField  	getNextTextField;

    // JList for queries whose results have arrived
    private JList               queryResultList;

    // Buttons
    private JButton     	qbeButton;
    private JButton     	queryExecuteButton;
    private JButton     	triggerButton;
    private JButton     	getNextButton;
    private JButton     	pauseButton;
    private JButton    		killButton;
    private JButton             expandButton;
    private JButton             detachButton;
    private JButton             qbeSEButton;

    // Misc components used in frame
    private JTree       	resultTree;
    private JSlider     	getNextSlider;
    private JComboBox   	queryListBox;
    private JProgressBar	progressBar;
    
    // Scroll Panes for query input and query output tree 
    private JScrollPane 	queryScrollPane;
    private JScrollPane 	resultTreeScrollPane;
    private JScrollPane         queryResultListScrollPane;

    // Text label for status info and choose query list box
    private JLabel              statusText;
    private JLabel              queryChooseLabel;
    private JLabel              resultCountLabel;

    // Layout and its constraints for the frame
    private GridBagLayout       frameLayout;
    private GridBagConstraints  frameLayoutC;

    private Vector              dtdList;
    private Vector              xmlqbe;

    // QueryExecution IF - submit query and get results from this interface
    private QueryExecutionIF    qeIF;

    // Used to determine if called by applet or stand-alone application
    private boolean             callType;

	// the applet context to display search engine results in a browser
	private AppletContext appletContext = null;
	
    private boolean             triggerNow;

    private DefaultMutableTreeNode    expandTreeRootNode;

    private DefaultListModel          queryResultListModel;

    // Dimensions of the frame
    public static final int WIDTH = 670;
    public static final int HEIGHT = 600;
    
    // Initial value of the slider - passed by default to the connection manager if no user selection
    public static final int SLIDER_INIT_VALUE = 20;

    // Internal padding used by the components in the panels - used for alignment and placement
    public static final int PAD = 10;

    //Ask server for a list of DTDs?
    private static boolean requestDTDList = true;
    
    
	// Constructor 1
	// Use this constructor only if the client is not run as an applet
	public NiagaraUI(String serverHostName, int port, boolean appl) 
		{
			this(serverHostName, port, appl, null);
		}
	
    // Constructor 2
    public NiagaraUI(String serverHostName, int port, boolean appl, AppletContext appletContext) {
	// Set title of frame
        super("XML Query Engine");

	// Get content pane of this frame and set layout
    	contentPane = this.getContentPane();
    	frameLayout = new GridBagLayout();
    	frameLayoutC = new GridBagConstraints();
    	contentPane.setLayout(frameLayout);
	
	// Set size of frame
        setSize(WIDTH,HEIGHT);

	// Center the frame on the screen
	Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
	Rectangle frameBounds = getBounds();

	setLocation((screenSize.width-frameBounds.width)/2,(screenSize.height-frameBounds.width)/2);

	// If appl=true: application, else called from applet
	callType = appl;

	// set the applet context (if any provided)
	this.appletContext = appletContext;

	// Create the menu bar and query input, output and status panels
    	createMenuBar(callType);
    	createQueryPanel();
    	createResultPanel();
	createStatusPanel();

	// Initially disable buttons still query is input
	disableQueryInputButtons();
	disableQueryResultButtons();

	// For closing of window(frame)
        addWindowListener(new WindowAdapter() {
            public void windowClosed(WindowEvent e) {
				// Call exitUI method to exit gracefully
				exitUI();
			}
			
        });

	// Connect to the connection manager and get the DTD list for QBE
	qeIF = new ConnectionManager(serverHostName,port,this);

	if (requestDTDList) 
	    dtdList = qeIF.getDTDList();
	else 
	    dtdList = new Vector();
	
	expandTreeRootNode = null;
	xmlqbe = null;

	// Display frame
        setVisible(true);	
    }
    
    // Main if UI run as an application
    public static void main(String[] argv) {
	// Check for correct number of arguments
	if (argv.length == 2 && argv[1].equals("-noDTDList")) {
	    requestDTDList = false;
	}
	else if(argv.length != 1){
	    System.out.println("Usage: Javarun client.NiagaraUI <server hostname> [-noDTDList]");
	    System.exit(0);
	}

	// Construct NiagaraUI object
	new NiagaraUI(argv[0], ConnectionManager.SERVER_PORT, true); 
    }

    // Create the file menu bar
    private void createMenuBar(boolean typeAppl) {
	menuBar = new JMenuBar();
        menuBar.setVisible(true);
	
        // File Menu
        fileMenu = new JMenu("File");
        fileMenu.setMnemonic(KeyEvent.VK_F);

	// If run as application add the open and save file menu options
	// If run as applet, don't create the open and save file menu options 
	// because of applet security issues
	if ( typeAppl ) {
	    // Load query from file
	    JMenuItem openQuery = new JMenuItem("Open Query...");
	    openQuery.setMnemonic(KeyEvent.VK_O);
	    fileMenu.add(openQuery);
	    openQuery.addActionListener(this);
	    
	    // Save query to file
	    JMenuItem saveQuery = new JMenuItem("Save Query...");
	    saveQuery.setMnemonic(KeyEvent.VK_S);
	    fileMenu.add(saveQuery);
	    saveQuery.addActionListener(this);
	}
	
	// Exit menu item
        JMenuItem exitProg = new JMenuItem("Exit");
        exitProg.setMnemonic(KeyEvent.VK_X);
        fileMenu.add(exitProg);
        exitProg.addActionListener(this);

        menuBar.add(fileMenu);
        setJMenuBar(menuBar);
    }

    // Create query input panel with components
    private void createQueryPanel() {
        GridBagLayout queryInputGrid = new GridBagLayout();
	queryPanel = new JPanel(queryInputGrid);
	Border queryBorder = BorderFactory.createCompoundBorder(BorderFactory.createRaisedBevelBorder(),BorderFactory.createLoweredBevelBorder());
	TitledBorder queryBorder1 = BorderFactory.createTitledBorder(queryBorder,"Query Input");
	queryBorder1.setTitleJustification(TitledBorder.CENTER);
	queryPanel.setBorder(queryBorder1);

	GridBagConstraints queryInputC = new GridBagConstraints();

        GridBagLayout tLayout = new GridBagLayout();
	GridBagConstraints tC = new GridBagConstraints();
	tPanel = new JPanel(tLayout);

	// Create and position the QBE XML button
        qbeButton = makeButton("Query",KeyEvent.VK_Q,"Open QBE Interface to Input Query");
        tC.gridx = 0;
        tC.gridy = 0;
        tC.weightx = 0.1;
        tC.weighty = 0.0;
        tC.insets = new Insets(0,PAD,PAD,PAD);
        tC.anchor = GridBagConstraints.NORTHWEST;
        tLayout.setConstraints(qbeButton,tC);
        tPanel.add(qbeButton);        

	// Create and position the QBE SE button
        qbeSEButton = makeButton("Search",KeyEvent.VK_S,"Open QBE Interface to Input Query");
        tC.gridx = 1;
        tC.gridy = 0;
        tC.weightx = GridBagConstraints.RELATIVE;
	tC.insets = new Insets(0,0,PAD,PAD);
        tC.anchor = GridBagConstraints.WEST;
        tLayout.setConstraints(qbeSEButton,tC);
        tPanel.add(qbeSEButton);        

	qbeButton.setPreferredSize(qbeSEButton.getPreferredSize());

	queryChooseLabel = new JLabel("Choose Query");
	tC.gridx = 2;
	tC.weightx = 1.0;
	tC.insets = new Insets(0,0,PAD,PAD);
	tC.anchor = GridBagConstraints.EAST;
	tLayout.setConstraints(queryChooseLabel,tC);
	tPanel.add(queryChooseLabel);

	// Create and position the choose query combo box
        queryListBox = new JComboBox();
	queryListBox.setRenderer(new ComboBoxRenderer());
        tC.gridx = 3;
	tC.weightx = GridBagConstraints.REMAINDER;
	tC.insets = new Insets(0,0,PAD,PAD);
        tC.anchor = GridBagConstraints.NORTHEAST;
        tLayout.setConstraints(queryListBox,tC);
        queryListBox.setEditable(false);
	QueryInfo q = new QueryInfo();
	q.setQueryID(-1);
	q.setQueryType("New Query");
        queryListBox.addItem(q);
        queryListBox.setToolTipText("Choose Query");
        queryListBox.addActionListener(this);
    	tPanel.add(queryListBox);

	queryInputC.gridx = 0;
        queryInputC.gridy = 0;
        queryInputC.weightx = 1.0;
        queryInputC.weighty = 0.0;
        queryInputC.gridwidth = GridBagConstraints.REMAINDER;
        queryInputC.insets = new Insets(0,0,0,0);
	queryInputC.fill = GridBagConstraints.BOTH;
	queryInputGrid.setConstraints(tPanel,queryInputC);
	queryPanel.add(tPanel);

    	GridBagLayout iLayout = new GridBagLayout();
	GridBagConstraints iC = new GridBagConstraints();
	iPanel = new JPanel(iLayout);

	// Create and position the query input area
        queryText = new JTextArea();
        queryScrollPane = new JScrollPane(queryText);
        queryScrollPane.setBorder(BorderFactory.createLineBorder(Color.black));
        iC.gridx = 0;
        iC.gridy = 0;
        iC.weightx = 0.85;
        iC.weighty = 1.0;
        iC.insets = new Insets(0,PAD,0,PAD);	    
        iC.fill = GridBagConstraints.BOTH;
        iLayout.setConstraints(queryScrollPane,iC);
	queryText.getDocument().addDocumentListener(this);
        queryText.setToolTipText("Input Query in this Area");
        iPanel.add(queryScrollPane);
	   
	queryResultListModel = new DefaultListModel();
	queryResultList = new JList(queryResultListModel);
	queryResultList.setCellRenderer(new ListRenderer());
	queryResultList.setBorder(BorderFactory.createLineBorder(Color.black));
	queryResultListScrollPane = new JScrollPane(queryResultList);
	iC.gridx = GridBagConstraints.RELATIVE;
	iC.weightx = 0.15;
	iC.insets = new Insets(0,0,0,PAD);
	iLayout.setConstraints(queryResultListScrollPane,iC);
	iPanel.add(queryResultListScrollPane);

	queryInputC.gridx = 0;
        queryInputC.gridy = 1;
        queryInputC.weightx = 1.0;
        queryInputC.weighty = 1.0;
        queryInputC.gridwidth = GridBagConstraints.REMAINDER;
        queryInputC.insets = new Insets(0,0,0,0);
	queryInputC.fill = GridBagConstraints.BOTH;
	queryInputGrid.setConstraints(iPanel,queryInputC);
	queryPanel.add(iPanel);

	GridBagLayout bLayout = new GridBagLayout();
	GridBagConstraints bC = new GridBagConstraints();
	bPanel = new JPanel(bLayout);

	// Create and position the trigger button
        triggerButton = makeButton("Install Trigger",KeyEvent.VK_T,"Install Trigger for Query");
        bC.gridx = 0;
        bC.gridy = 0;
        bC.weightx = 0.5;
        bC.weighty = 1.0;
        bC.insets = new Insets(PAD,PAD,PAD,PAD);
	bC.fill = GridBagConstraints.BOTH;
        bLayout.setConstraints(triggerButton,bC);
        bPanel.add(triggerButton);

	// Create and position the execute button
        queryExecuteButton = makeButton("Execute Query",KeyEvent.VK_E,"Execute Query");
        bC.gridx = 1;
	bC.insets = new Insets(PAD,PAD,PAD,PAD);
        bLayout.setConstraints(queryExecuteButton,bC);
	bPanel.add(queryExecuteButton);

        queryInputC.gridx = 0;
        queryInputC.gridy = 2;
        queryInputC.weightx = 1.0;
        queryInputC.weighty = 0.0;
        queryInputC.gridwidth = GridBagConstraints.REMAINDER;
        queryInputC.insets = new Insets(0,0,0,0);
        queryInputC.anchor = GridBagConstraints.SOUTH;
	queryInputC.fill = GridBagConstraints.BOTH;
	queryInputGrid.setConstraints(bPanel,queryInputC);
	queryPanel.add(bPanel);

	// Position the query input panel
     	frameLayoutC.gridx = 0;
     	frameLayoutC.gridy = 0;
     	frameLayoutC.weightx = 1.0;
     	frameLayoutC.weighty = 0.5;
     	frameLayoutC.gridheight = 1;
     	frameLayoutC.ipady = (HEIGHT / 5);
     	frameLayoutC.fill = GridBagConstraints.BOTH;
     	frameLayout.setConstraints(queryPanel, frameLayoutC);
	contentPane.add(queryPanel); 
    }

    // Create query result panel with components
    private void createResultPanel() {
        GridBagLayout queryResultGrid = new GridBagLayout();
	resultPanel = new JPanel(queryResultGrid);
	Border resultBorder = BorderFactory.createCompoundBorder(BorderFactory.createRaisedBevelBorder(),BorderFactory.createLoweredBevelBorder());
	TitledBorder resultBorder1= BorderFactory.createTitledBorder(resultBorder,"Query Result");
	resultBorder1.setTitleJustification(TitledBorder.CENTER);
	resultPanel.setBorder(resultBorder1);
	GridBagConstraints queryResultC = new GridBagConstraints();

	// Create and position the result tree area
	resultTree = new JTree(new DefaultMutableTreeNode());
	resultTreeScrollPane = new JScrollPane(resultTree);
	resultTreeScrollPane.setBorder(BorderFactory.createLineBorder(Color.black));
	queryResultC.gridx = 0;
        queryResultC.gridy = 0;
        queryResultC.weightx = 0.80;
        queryResultC.weighty = 1.0;
        queryResultC.gridheight = 5;
        queryResultC.insets = new Insets(PAD,PAD,PAD,PAD);
        queryResultC.fill = GridBagConstraints.BOTH;
        queryResultC.anchor = GridBagConstraints.NORTHWEST;
        queryResultGrid.setConstraints(resultTreeScrollPane,queryResultC);
        resultTree.setToolTipText("Result of Query");
        resultPanel.add(resultTreeScrollPane);
	    
	// Create and position the get next text field
        getNextTextField = new JTextField("20");
        queryResultC.gridx = 1;
        queryResultC.gridy = 0;
        queryResultC.weightx = 0.20;
        queryResultC.weighty = 0.0;
        queryResultC.gridheight = 1;
        queryResultC.gridwidth = 2;
        queryResultC.insets = new Insets(PAD,0,0,PAD);
        queryResultC.fill = GridBagConstraints.HORIZONTAL;
        queryResultGrid.setConstraints(getNextTextField,queryResultC);
        getNextTextField.setToolTipText("Enter the desired number of results");
        resultPanel.add(getNextTextField);
	    
	// Create and position the slider
        getNextSlider = new JSlider(JSlider.HORIZONTAL,0,100,SLIDER_INIT_VALUE);
        getNextSlider.setMajorTickSpacing(50);
        getNextSlider.setMinorTickSpacing(25);
        getNextSlider.setPaintTicks(true);
        getNextSlider.setPaintLabels(true);
        getNextSlider.setBorder(BorderFactory.createLoweredBevelBorder());
        getNextSlider.setToolTipText("Choose the desired number of results");
        queryResultC.gridx = 1;
        queryResultC.gridy = GridBagConstraints.RELATIVE;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 0.0;
        queryResultC.insets = new Insets(0,0,0,PAD);
        queryResultC.fill = GridBagConstraints.HORIZONTAL;
        queryResultGrid.setConstraints(getNextSlider,queryResultC);
        getNextSlider.addChangeListener(this);
        resultPanel.add(getNextSlider);

	// Create and position the get next button
        getNextButton = makeButton("Get Next",KeyEvent.VK_G,"Get the next 'n' results");
        queryResultC.gridx = 1;
        queryResultC.gridy = GridBagConstraints.RELATIVE;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 0.0;
        queryResultC.fill = GridBagConstraints.HORIZONTAL;
        queryResultGrid.setConstraints(getNextButton,queryResultC);
        resultPanel.add(getNextButton);
		
		
	// Create and position the pause/resume button
        pauseButton = makeButton("Pause",KeyEvent.VK_P,"Pause Query Execution");
        queryResultC.gridx = 1;
        queryResultC.gridy = 3;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 1.0;
        queryResultC.gridwidth = 2;
        queryResultC.anchor = GridBagConstraints.CENTER;
        queryResultC.insets = new Insets(PAD,0,PAD,PAD);
        queryResultGrid.setConstraints(pauseButton,queryResultC);
        resultPanel.add(pauseButton);
	    
	// Create and position the kill query button
        killButton = makeButton("Kill Query",KeyEvent.VK_K,"Stop and Kill Query Execution");
        queryResultC.gridx = 1;
        queryResultC.gridy = 4;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 0.0;
        queryResultC.gridwidth = 2;
        queryResultC.anchor = GridBagConstraints.SOUTH;
        queryResultGrid.setConstraints(killButton,queryResultC);
        resultPanel.add(killButton);

	expandButton = makeButton("Expand Tree",KeyEvent.VK_X,"Expand the Result Tree");
	queryResultC.gridx = 0;
        queryResultC.gridy = 5;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 0.0;
        queryResultC.gridwidth = 1;
	queryResultC.insets = new Insets(0,PAD,PAD,PAD);
	queryResultC.fill = GridBagConstraints.NONE;
        queryResultC.anchor = GridBagConstraints.SOUTHWEST;
	queryResultGrid.setConstraints(expandButton,queryResultC);
	resultPanel.add(expandButton);

	//detachButton = makeButton("Detach",KeyEvent.VK_D,"Display the Result Tree in a New Frame");
	resultCountLabel = new JLabel("Total Results: 0");
	queryResultC.gridx = 0;
        queryResultC.gridy = 5;
        queryResultC.weightx = 0.0;
        queryResultC.weighty = 0.0;
        queryResultC.gridwidth = 1;
        queryResultC.anchor = GridBagConstraints.SOUTHEAST;
	queryResultGrid.setConstraints(resultCountLabel,queryResultC);
	resultPanel.add(resultCountLabel);
	
	// Position the query output panel
     	frameLayoutC.gridy = 1;
     	frameLayoutC.weighty = 0.5;
     	frameLayoutC.ipady = 0;
     	frameLayout.setConstraints(resultPanel, frameLayoutC);
        contentPane.add(resultPanel);
    }

    // Create the status panel
    private void createStatusPanel() {
        statusPanel = new JPanel(new BorderLayout());
        statusPanel.setBorder(BorderFactory.createLoweredBevelBorder());
     	frameLayoutC.gridy = 2;
     	frameLayoutC.weightx = 1.0;
     	frameLayoutC.weighty = 0.0;
        frameLayoutC.fill = GridBagConstraints.HORIZONTAL;
     	frameLayout.setConstraints(statusPanel,frameLayoutC);
     	
	statusText = new JLabel("Ready");
	statusPanel.add(statusText);
        contentPane.add(statusPanel);
    }

    // Method to create a button
    private JButton makeButton(String buttonText, int keyBoardMnemonic, String toolTipText) {
        JButton button = new JButton(buttonText);
	button.setMnemonic(keyBoardMnemonic);
	button.setToolTipText(toolTipText);
	button.addActionListener(this);
	return button;
    }    

    public synchronized void actionPerformed( ActionEvent e ) {
        String actionCommand = e.getActionCommand();

	// File Menu: Exit
        if ( actionCommand.equals("Exit") ) {
	    exitUI();
	}
	else 
	// File Menu: Open Query
        if ( actionCommand.equals("Open Query...") ) {
            FileDialog openFileDialog = new FileDialog(this, "Open Query", FileDialog.LOAD);
            openFileDialog.setDirectory(".");
            openFileDialog.setFile("*.qry");
            openFileDialog.setFilenameFilter(new queryFileFilter("qry"));
            openFileDialog.setModal(true);
            openFileDialog.setVisible(true);
            String queryFileName = openFileDialog.getFile();
	    
	    if ( queryFileName == null || queryFileName.equals("") ) return;

	    try {
		String queryFromFile = new String();
	    	String lineFromFile;
	 	BufferedReader queryOpenFile = new BufferedReader(new FileReader(queryFileName));
	    	while ( (lineFromFile = queryOpenFile.readLine()) != null ) {
		    queryFromFile += lineFromFile;
		    queryFromFile += "\n";
		}
		    queryText.setText(queryFromFile);
		    queryText.setCaretPosition(0);
	    } catch ( java.io.FileNotFoundException eFileNotFound ) { System.out.println("IO Exception: File Not Found"); }
              catch ( java.io.IOException eIO ) { System.out.println("IO Exception: Open File"); }
       }
	else 
	// File Menu: Save Query
       if ( actionCommand.equals("Save Query...") ) {
	   FileDialog saveFileDialog = new FileDialog(this, "Save Query", FileDialog.SAVE);
	   saveFileDialog.setDirectory(".");
           saveFileDialog.setFile("*.qry");
           saveFileDialog.setFilenameFilter(new queryFileFilter("qry"));
           saveFileDialog.setModal(true);
           saveFileDialog.setVisible(true);
           String queryFileName = saveFileDialog.getFile();
	   
	   if ( queryFileName == null || queryFileName.equals("") ) return;

	   try {
	       BufferedWriter querySaveFile = new BufferedWriter(new FileWriter(queryFileName));
	       String queryOut = new String(queryText.getText());	       
	       querySaveFile.write(queryOut,0,queryOut.length());
	       querySaveFile.flush();
	       querySaveFile.close();
	   } catch ( java.io.IOException eIO ) { System.out.println("IO Exception: Save File"); }
	   
       }
       else
       // QBE button action
       if ( actionCommand.equals(qbeButton.getText()) ) {
	   StringBuffer result = new StringBuffer();
	   XmlInterface qbePopup = new XmlInterface(null,true,result,dtdList,this.qeIF);
	   if ( !result.toString().equals("") )
	       queryText.setText(result.toString());
	   queryText.setCaretPosition(0);
	   
	   //qbePopup.dispose();
       }
       else
	   // Pause/Resume button action
       if ( actionCommand.equals(pauseButton.getText()) ) {
	   if ( pauseButton.getText().equals("Pause") ) {
	       int activeQuery;
	       if ( (activeQuery = getActiveQuery()) > 0 ) {
		   if ( qeIF.isResultFinal(activeQuery) ) {
		       Toolkit.getDefaultToolkit().beep();
		       JOptionPane.showMessageDialog(null,"All Results for Query Obtained","Niagara",JOptionPane.INFORMATION_MESSAGE);
		   }
		   else {
		       pauseButton.setText("Resume");
		       pauseButton.setMnemonic(KeyEvent.VK_R);
		       pauseButton.setToolTipText("Resume Query Execution");
		       getNextButton.setEnabled(false);
		       qeIF.suspendQuery(activeQuery);
		   }
	       }
	       else {
		   Toolkit.getDefaultToolkit().beep();
		   JOptionPane.showMessageDialog(null,"Error: No Active Query","Error",JOptionPane.ERROR_MESSAGE); 
	       }
	   }
	   else {
	       pauseButton.setText("Pause");
	       pauseButton.setMnemonic(KeyEvent.VK_P);
	       pauseButton.setToolTipText("Pause Query Execution");
	       getNextButton.setEnabled(true);
	       
	       int activeQuery;
	       if ( (activeQuery = getActiveQuery()) > 0 ) {
		   qeIF.resumeQuery(activeQuery);
	       }
	       else {
		   Toolkit.getDefaultToolkit().beep();
		   JOptionPane.showMessageDialog(null,"Error: No Active Query","Error",JOptionPane.ERROR_MESSAGE); 
	       }
	   }
       }
       else
       /** Execute Query button action
	 */
       if ( actionCommand.equals(queryExecuteButton.getText()) ) {
	   int queryId;
	   int getNResults;

	   if ( queryText.getText().equals("") ) {
	       Toolkit.getDefaultToolkit().beep();
	       JOptionPane.showMessageDialog(null,"Error: No Query to Execute","Query Input Error",JOptionPane.ERROR_MESSAGE);
	       return;
	   }

	   QueryInfo queryType = new QueryInfo();

	   if ( getNextTextField.getText().equals("") ) 
	       getNResults = SLIDER_INIT_VALUE;
	   else
	       getNResults = Integer.parseInt(getNextTextField.getText());

	   if ( queryText.getText().indexOf("CREATE TRIGGER") != -1 ) {
	       queryId = qeIF.executeTriggerQuery(queryText.getText(), getNResults);  
	       queryType.setQueryType("Trigger");
	   }
	   else
	       if ( queryText.getText().indexOf("WHERE") != -1 ) {
		   queryId = qeIF.executeQuery(queryText.getText(), getNResults);
		   queryType.setQueryType("XMLQL");
	       }
	       else {
		   queryId = qeIF.executeSEQuery(queryText.getText(), getNResults);
		   queryType.setQueryType("SE");
	       }
	   
	   queryType.setQueryID(queryId);
	   
	   queryListBox.addItem(queryType);
	   queryListBox.setSelectedItem(queryType);
	   
	   if ( triggerNow ) {
	       QueryInfo qType = new QueryInfo();
	       queryId = qeIF.executeQuery(queryText.getText(), getNResults);
	       qType.setQueryID(queryId);
	       qType.setQueryType("XMLQL T");
	       queryListBox.addItem(qType);
	       queryListBox.setSelectedItem(qType);
	   }
	   
	   disableQueryInputButtons();
	   enableQueryResultButtons();
	   
	   triggerNow = false;
       }
       else
       /** Kill Query button action
	 */
	   if ( actionCommand.equals(killButton.getText()) ) {
	       if ( getActiveQuery() > 0 ) {
		   QueryInfo query = (QueryInfo)queryListBox.getSelectedItem();

		   if(!qeIF.isResultFinal(getActiveQuery())){
		       qeIF.killQuery(getActiveQuery());
		   }

		   queryListBox.removeItemAt(queryListBox.getSelectedIndex());

		   if ( queryResultListModel.contains(query) ) queryResultListModel.removeElement(query);

		   if ( getActiveQuery() < 0 ) {
		       queryText.setText("");
		       showResults(new DefaultMutableTreeNode());
		       statusText.setText("Ready");
		       disableQueryInputButtons();
		       disableQueryResultButtons();
		   }
	       }
	       else {
		   Toolkit.getDefaultToolkit().beep();
		   JOptionPane.showMessageDialog(null,"No Active Query to Killl","Niagara",JOptionPane.ERROR_MESSAGE);
	       }
	   }
       else
       /** Choose query from combo box action
	 */
       if ( e.getSource() instanceof JComboBox ) {
	   int qID;
	   QueryInfo query = (QueryInfo)((JComboBox)e.getSource()).getSelectedItem();
	   if ( (qID = getActiveQuery()) > 0 ) {
	       statusText.setText("Getting status...");
	       showResults(qeIF.getQueryResultTree(qID));

	       if ( qeIF.isResultFinal(qID) )
		   statusText.setText("All results for Query "+getActiveQuery()+" have arrived");
	       else
		   statusText.setText("Processing query "+getActiveQuery()+"...");

	       queryText.setText(qeIF.getQueryString(qID));
	       queryText.setCaretPosition(0);
	       enableQueryResultButtons();

	       if ( queryResultListModel.contains(query) ) queryResultListModel.removeElement(query);
	   }
	   else
	       clearInputArea();
       }
       else
       /**
	 * Get next button action
	 */
       if ( actionCommand.equals(getNextButton.getText()) ) {
	   if ( getActiveQuery() > 0 ) {
	       if ( qeIF.isResultFinal(getActiveQuery()) ) {
		    Toolkit.getDefaultToolkit().beep();
		    JOptionPane.showMessageDialog(null,"All Results for Query Obtained","Niagara",JOptionPane.INFORMATION_MESSAGE);
	       }
	       else {
		   int getNResults = Integer.parseInt(getNextTextField.getText());
		   qeIF.getNext(getActiveQuery(),getNResults);
	       }
	   } 
	   else {
	       Toolkit.getDefaultToolkit().beep();
	       JOptionPane.showMessageDialog(null,"Error: No Active Query","Error",JOptionPane.ERROR_MESSAGE);
	   }
       } 
       else
       /** Trigger button action
	 */
       if ( actionCommand.equals(triggerButton.getText()) ) {
	   TriggerUI trigger = new TriggerUI(null,true);

	   String trigName = trigger.getTriggerName();

	   if ( trigName != null ) {
	       queryText.insert(trigName,0);
	       queryText.append(trigger.getTriggerString());
	       triggerButton.setEnabled(false);
	   }

	   if ( trigger.isInstallNow() ) triggerNow = true;
	   trigger.dispose();
       }
       else
	   if ( actionCommand.equals(expandButton.getText()) ) {
	       if ( expandTreeRootNode != null ) {
		   int children = expandTreeRootNode.getChildCount();
		   int depth = expandTreeRootNode.getDepth();
		   int rowCount = resultTree.getRowCount();

		   for ( int i = 0 ; i < rowCount*children*depth ; i++ )
		       resultTree.expandRow(i);
	       }
	   } else
	       if ( actionCommand.equals(qbeSEButton.getText()) ) {
		    StringBuffer result = new StringBuffer();
		    SEInterface qbeSEPopup = new SEInterface(null,true,result,dtdList,this.qeIF);
		    queryText.setText(result.toString());
		    queryText.setCaretPosition(0);
	       }
    }

    public void clearInputArea() {
	queryText.setText("");
	showResults(new DefaultMutableTreeNode());
	disableQueryInputButtons();
	disableQueryResultButtons();
	getNextTextField.setText("20");
	getNextSlider.setValue(SLIDER_INIT_VALUE);
	statusText.setText("Ready");
    }

    // Handle slider changed events
    public void stateChanged(ChangeEvent e ) {
        JSlider source = (JSlider)e.getSource();
        if ( source.getValueIsAdjusting() ) {
            int value = (int)source.getValue();
            getNextTextField.setText(String.valueOf(value));
        }
    }

    // Exit action
    private void exitUI() {	
		
	this.setVisible(false);
	
	// End session with the connection manager
	qeIF.endSession();

	this.dispose();

	// If run as application dispose this object and exit also
	if(callType) System.exit(0);
    }

    // Return selected query id from the choose query combo box, -1 if none
    private int getActiveQuery() { 
	if ( queryListBox.getSelectedIndex() == 0 ) return -1;
	
	return ((QueryInfo)queryListBox.getSelectedItem()).getQueryID();
    }

    // Display results to the result area
    private void showResults(DefaultMutableTreeNode rootNode) { 
	expandTreeRootNode = rootNode;
	resultTree = new JTree(rootNode);
	resultTree.setVisible(true);
	resultTreeScrollPane.getViewport().add(resultTree);
	resultTreeScrollPane.setVisible(true);
	resultTreeScrollPane.validate();
	
	resultCountLabel.setText("Total Results: "+rootNode.getChildCount());
	
	MouseListener ml = new MouseAdapter() {
	    public void mouseClicked(MouseEvent e) {
		int selRow = resultTree.getRowForLocation(e.getX(), e.getY());
		if( (selRow != -1) && (e.getClickCount() == 2) )
		    showURLDocument();
	    }
	};
	resultTree.addMouseListener(ml);
    }
    
    public void notifyNew(int id) { 
	if ( id == getActiveQuery() )
	    showResults(qeIF.getQueryResultTree(id));
	
	QueryInfo activeQ = getQueryInfoObject(id);
	if ( activeQ != null && queryResultListModel.contains(activeQ) == false) 
	    queryResultListModel.addElement(activeQ);
	
	statusText.setText("New results for Query "+id+" have arrived");
    }
    
    public QueryInfo getQueryInfoObject(int qID) {
	for ( int i = 1 ; i < queryListBox.getItemCount() ; i++ ) {
	    int queryID = ((QueryInfo)queryListBox.getItemAt(i)).getQueryID();
	    if ( queryID == qID ) return (QueryInfo)queryListBox.getItemAt(i);
	}
	
	return null;
    }
    
    public void notifyFinalResult(int id) {
	Toolkit.getDefaultToolkit().beep();
	
	if ( id == getActiveQuery() )
	    showResults(qeIF.getQueryResultTree(id));
	
	QueryInfo activeQ = getQueryInfoObject(id);
	int index = queryResultListModel.indexOf(activeQ);

	if ( index < 0 ) 
		queryResultListModel.addElement(activeQ);
	else
		queryResultListModel.setElementAt(activeQ,index);
	
	statusText.setText("All results for Query "+id+" have arrived");	
    }

    public void errorMessage(String err) { 
	Toolkit.getDefaultToolkit().beep();
	JOptionPane.showMessageDialog(contentPane,err,"Error",JOptionPane.ERROR_MESSAGE);
    }

    public void disableQueryResultButtons() {
	getNextButton.setEnabled(false);
	pauseButton.setEnabled(false);
	killButton.setEnabled(false);
	expandButton.setEnabled(false);
    }

    public void enableQueryResultButtons() {
	getNextButton.setEnabled(true);
	pauseButton.setEnabled(true);
	killButton.setEnabled(true);
	expandButton.setEnabled(true);
    }

    public void disableQueryInputButtons() {
	triggerButton.setEnabled(false);
	queryExecuteButton.setEnabled(false);
    }

    public void enableQueryInputButtons() {
	triggerButton.setEnabled(true);
	queryExecuteButton.setEnabled(true);
    }

    public void showURLDocument() {
		
		System.out.println("Fetching the URL");
		
		DefaultMutableTreeNode urlNode = (DefaultMutableTreeNode)resultTree.getLastSelectedPathComponent();
	
		if ( urlNode == null ) return; // there is no url to show

		String url = (String)urlNode.getUserObject(); // get the url
		if(callType == true){
			// Application pops up a window
			SEURLContent seURLContent = new SEURLContent(url);
		} else {
			// applet displays url in the browser
			try{
				appletContext.showDocument(new URL(url),"_blank");
			}
			catch(java.net.MalformedURLException e){
				System.err.println("Malformed URL");
			}
		}
	
    }

    // Methods implemented for the DocumentListener Interface - enable the 
    // buttons in the query input panel whenever query text changes
    public void insertUpdate(DocumentEvent e) {
	enableQueryInputButtons();
    }

    public void removeUpdate(DocumentEvent e) {
	enableQueryInputButtons();
    }

    public void changedUpdate(DocumentEvent e) {
	enableQueryInputButtons();
    }
    
    // Class for file filter - select only query (*.qry) files
    public class queryFileFilter implements FilenameFilter {
	String fileExt;

        public queryFileFilter (String ext) {
	    fileExt = "." + ext;
	}

	public boolean accept(File dir, String name) {
	    return name.endsWith(fileExt);
	}
    }

    public class QueryInfo {
	int queryID;
	String queryType;

	QueryInfo() { }

	public int getQueryID() {
	    return queryID;
	}

	public String getQueryType() {
	    return queryType;
	}

	public void setQueryID(int qID) {
	    queryID = qID;
	}

	public void setQueryType(String type) {
	    queryType = new String(type);
	}
    }

    public class ComboBoxRenderer extends JLabel implements ListCellRenderer {
	public ComboBoxRenderer() { }

	public Component getListCellRendererComponent(
						      JList list,
						      Object value,
						      int index,
						      boolean isSelected,
						      boolean cellHasFocus)
	{	    
	    QueryInfo query = (QueryInfo)value;
	    if ( query.getQueryID() > 0 )
		setText(String.valueOf(query.getQueryID())+" "+query.getQueryType());
	    else
		setText(query.getQueryType());
	    return this;
	}	
    }

    public class ListRenderer extends JLabel implements ListCellRenderer {
	public ListRenderer() { }

	public Component getListCellRendererComponent(
						      JList list,
						      Object value,
						      int index,
						      boolean isSelected,
						      boolean cellHasFocus)
	{	    
	    QueryInfo query = (QueryInfo)value;

	    if ( qeIF.isResultFinal(query.getQueryID()) )
		this.setForeground(Color.red);
	    else
		this.setForeground(Color.blue);

	    setText(String.valueOf(query.getQueryID())+" "+query.getQueryType());
	    
	    return this;
	}	
    }
}
