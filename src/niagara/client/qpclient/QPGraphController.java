/*
 * $Id: QPGraphController.java,v 1.2 2000/10/07 01:08:07 vpapad Exp $
 *
 * Copyright (c) 1998 The Regents of the University of California.
 * All rights reserved.  See the file COPYRIGHT for details.
 */
package niagara.client.qpclient;
import diva.graph.*;

import diva.graph.model.*;

import diva.canvas.Figure;
import diva.canvas.FigureLayer;
import diva.canvas.GraphicsPane;
import diva.canvas.Site;

import diva.canvas.connector.AutonomousSite;
import diva.canvas.connector.CenterSite;
import diva.canvas.connector.PerimeterSite;
import diva.canvas.connector.PerimeterTarget;
import diva.canvas.connector.Connector;
import diva.canvas.connector.ConnectorManipulator;
import diva.canvas.connector.ConnectorEvent;
import diva.canvas.connector.ConnectorListener;
import diva.canvas.connector.ConnectorTarget;

import diva.canvas.event.LayerAdapter;
import diva.canvas.event.LayerEvent;
import diva.canvas.event.MouseFilter;

import diva.canvas.interactor.Interactor;
import diva.canvas.interactor.AbstractInteractor;

import diva.canvas.manipulator.GrabHandle;

import diva.canvas.selection.SelectionInteractor;
import diva.canvas.selection.SelectionModel;
import diva.canvas.selection.SelectionDragger;

import diva.util.Filter;

import java.awt.event.InputEvent;
import java.util.HashMap;

/**
 * A basic implementation of GraphController, which works with
 * simple graphs that have edges connecting simple nodes. It
 * sets up some simple interaction on its view's pane.
 *
 * @author 	Michael Shilman (michaels@eecs.berkeley.edu)
 * @version	$Revision: 1.2 $
 * @rating      Red
 */
public class QPGraphController extends AbstractGraphController {
 
    /** The selection interactor for drag-selecting nodes
     */
    private SelectionDragger _selectionDragger;

   /** The interactor for creating new nodes
     */
   
    private NodeCreator _nodeCreator;

   /** The interactor that interactively creates edges
     */
    private EdgeCreator _edgeCreator;

    /** The filter for control operations
     */
    private MouseFilter _controlFilter = new MouseFilter (
            InputEvent.BUTTON1_MASK,
            InputEvent.CTRL_MASK);

    /**
     * Create a new basic controller with default node and edge interactors.
     */
    public QPGraphController () {
        // The interactors attached to nodes and edges
        SelectionModel sm = getSelectionModel();
        NodeInteractor ni = new NodeInteractor(this, sm);
        EdgeInteractor ei = new EdgeInteractor(this, sm);
        setNodeInteractor(ni);
        setEdgeInteractor(ei);

        // Create and set up the target for connectors
        /*
        ct.setFigureFilter(new Filter() {
            public boolean accept (Object o) {
                Figure f = (Figure)o
                return (f.getUserObject() instanceof Node) ||
                    (f instanceof FigureWrapper &&
                            ((FigureWrapper)f).getChild().instanceof Node);
            }
        });
        */
        PerimeterTarget ct = new PerimeterTarget() {
	    public boolean accept (Figure f) {
                return (f.getUserObject() instanceof Node);
		// FIXME Used to also have ||
		// (f instanceof FigureWrapper &&
                //             ((FigureWrapper)f).getChild().instanceof Node);
            }
	};
        setConnectorTarget(ct);

        // Create and set up the manipulator for connectors
        ConnectorManipulator manipulator = new ConnectorManipulator(); // XXX
        manipulator.setSnapHalo(4.0);
        manipulator.setConnectorTarget(ct);
	manipulator.addConnectorListener(new EdgeDropper()); // XXX
        ei.setSelectionManipulator(manipulator);

        // The mouse filter needs to accept regular click or control click
        MouseFilter handleFilter = new MouseFilter(1, 0, 0);
        manipulator.setHandleFilter(handleFilter);
    }

    /**
     * Initialize all interaction on the graph pane. By the time
     * this method is called, all relevant references to views,
     * panes, and roles must already have been set up.
     */
    public void initializeInteraction () {
        GraphicsPane pane = getGraphView().getGraphicsPane();

        // Create and set up the selection dragger
        SelectionDragger _selectionDragger = new SelectionDragger(pane);
        _selectionDragger.addSelectionInteractor(getEdgeInteractor());
        _selectionDragger.addSelectionInteractor(getNodeInteractor());

        // Create a listener that creates new nodes // XXX
//          _nodeCreator = new NodeCreator();
//          _nodeCreator.setMouseFilter(_controlFilter);
//  	pane.getBackgroundEventLayer().addInteractor(_nodeCreator);

//          // Create the interactor that drags new edges. // XXX
//          _edgeCreator = new EdgeCreator();
//          _edgeCreator.setMouseFilter(_controlFilter);
//  	getNodeInteractor().addInteractor(_edgeCreator);

	PropertyEditorInteractor _propertyEditor = 
	    new PropertyEditorInteractor();
	_propertyEditor.setMouseFilter(new MouseFilter(2));
	getNodeInteractor().addInteractor(_propertyEditor);
    }

    ///////////////////////////////////////////////////////////////
    //// NodeCreator

    /** An inner class that places a node at the clicked-on point
     * on the screen, if control-clicked with mouse button 1. This
     * needs to be made more customizable.
     */
    protected class NodeCreator extends AbstractInteractor {
	String nodeType = "scan";
        public void mousePressed(LayerEvent e) {
	    if (nodeType == null)
		return;

            GraphView view = getGraphView();
            GraphModel model = getGraphModel();

            // Create a new node
            Node n = QPNodeFactory.getNode(nodeType);

            // Create a figure for it
            Figure nf = view.getNodeRenderer().render(n);
            nf.setInteractor(getNodeInteractor());
 
            // Add to the view and model
            view.addNodeFigureToLayer(nf);
            view.placeNodeFigure(nf, e.getLayerX(), e.getLayerY());
            view.addNodeMapping(n, nf);
            model.addNode(n);
        }
    }

    public void addNode(Node n) {
            GraphView view = getGraphView();
            GraphModel model = getGraphModel();

            // Create a figure for it
            Figure nf = view.getNodeRenderer().render(n);
            nf.setInteractor(getNodeInteractor());
 
            // Add to the view and model
            view.addNodeFigureToLayer(nf);
            view.placeNodeFigure(nf, 0, 0);
            view.addNodeMapping(n, nf);
            model.addNode(n);
    }
    ///////////////////////////////////////////////////////////////
    //// EdgeDropper

    /** An inner class that handles interactive changes to connectivity.
     */
    protected class EdgeDropper implements ConnectorListener {
        /**
         * Do nothing.
         */
        public void connectorDragged(ConnectorEvent evt) {}

        /**
         * Called when a connector end is dropped--attach or
         * detach the edge as appropriate.
         *
         * is currently snapped to a target, the target can be obtained
         * from the event as the source field.
         */
        public void connectorDropped(ConnectorEvent evt) {
            Connector c = evt.getConnector();
            Figure f = evt.getTarget();
            Edge e = (Edge)c.getUserObject();
            Node n = (f == null) ? null : (Node)f.getUserObject();
            GraphModel model = getGraphModel();
            switch (evt.getEnd()) {
            case ConnectorEvent.HEAD_END:
		if (n != null)
		    model.setEdgeHead(e, n);
		else
		    model.setEdgeHead(e, oldhead);
                break;
            case ConnectorEvent.TAIL_END:
		if (n != null)
		    model.setEdgeTail(e, n);
		else
		    model.setEdgeTail(e, oldtail);
                break;
            default:
                throw new IllegalStateException(
                        "Cannot handle both ends of an edge being dragged.");
            }
        }
    
        /**
         * Do nothing.
         */
        public void connectorSnapped(ConnectorEvent evt) {}

        /**
         * Do nothing.
         */
	private Node oldhead, oldtail;
        public void connectorUnsnapped(ConnectorEvent evt) {
	    oldhead = ((Edge) evt.getConnector().getUserObject()).getHead();
	    oldtail = ((Edge) evt.getConnector().getUserObject()).getTail();
	}
    }

    ///////////////////////////////////////////////////////////////
    //// EdgeCreator

    /** An interactor that interactively drags edges from one node
     * to another.
     */
    protected class EdgeCreator extends AbstractInteractor {
        public void mousePressed(LayerEvent e) {
            GraphView view = getGraphView();
            GraphModel model = getGraphModel();

            Figure source = e.getFigureSource();
            FigureLayer layer = (FigureLayer) e.getLayerSource();
            double x = e.getLayerX();
            double y = e.getLayerY();

            // Create a new edge
            Edge edge = model.createEdge();

            // Create a figure for it
            Site tailSite = getConnectorTarget().getTailSite(source, x, y);
            Site headSite = new AutonomousSite(layer, x, y);
            Connector ef = view.getEdgeRenderer().render(edge, tailSite, headSite);
            ef.setInteractor(getEdgeInteractor());

            // Add to the view and model
            view.addEdgeFigureToLayer(ef);
            view.addEdgeMapping(edge, ef);
            model.setEdgeTail(edge, (Node) source.getUserObject());

            // Add it to the selection so it gets a manipulator, and
            // make events go to the grab-handle under the mouse
            getSelectionModel().addSelection(ef);
            ConnectorManipulator cm = (ConnectorManipulator) ef.getParent();
            GrabHandle gh = cm.getHeadHandle();
            layer.grabPointer(e, gh);
        }
    }

    protected class PropertyEditorInteractor extends AbstractInteractor {
        public void mousePressed(LayerEvent e) {
            GraphView view = getGraphView();
            GraphModel model = getGraphModel();

            Figure source = e.getFigureSource();
	    QPNode qpn = (QPNode) source.getUserObject();
	    new PropertyEditor(null, qpn.getType() + ": " + (String) qpn.getProperty("id"), qpn);
        }
    }
}
