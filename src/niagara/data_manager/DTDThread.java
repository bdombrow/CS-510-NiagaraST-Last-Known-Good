/* $Id$ */

package niagara.data_manager;

import org.w3c.dom.*;
import java.io.*;
import java.util.*;

import org.xml.sax.*;

import niagara.query_engine.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.ResourceOp;
import niagara.xmlql_parser.op_tree.dtdScanOp;
import niagara.connection_server.NiagraServer;
import niagara.ndom.*;
import niagara.optimizer.colombia.*;

/** DTDThread provides a SourceThread compatible interface to the 
 *  DM fetch functionality */
public class DTDThread extends SourceThread {
    // Optimization-time attributes
    private Attribute variable;
    private Vector urls;

    private DataManager dm;
    private SinkTupleStream outputStream;

    public DTDThread() {
    };

    public DTDThread(Attribute variable, Vector urls) {
        this.variable = variable;
        this.urls = urls;
    }

    public void plugIn(SinkTupleStream outputStream, DataManager dm) {
        this.outputStream = outputStream;
        this.dm = dm;
    }

    /**
     * Thread run method
     *
     */
    public void run() {
        try {
            if (!dm.getDocuments(urls, null, outputStream))
                System.err.println("XXX vpapad dtdscan borked");
        } catch (ShutdownException se) {
            throw new PEException("XXX vpapad what do i do here?!");
        }
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        // XXX vpapad: assume constant parsing cost per document
        return new Cost(
            urls.size()
                * (catalog.getDouble("document_parsing_cost")
                    + catalog.getDouble("tuple_construction_cost")));
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
     */
    public void opInitFrom(LogicalOp op) {
        dtdScanOp dop = (dtdScanOp) op;
        urls = dop.getDocs();
        variable = dop.getVariable();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        return new DTDThread(variable, urls);
    }

    public int hashCode() {
        return urls.hashCode() ^ variable.hashCode();
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof DTDThread))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        return urls.equals(((DTDThread) o).urls)
            && variable.equals(((DTDThread) o).variable);
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        ;
    }

    public TupleSchema getTupleSchema() {
        TupleSchema ts = new TupleSchema();
        ts.addMapping(variable);
        return ts;
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpAttributesInXML(StringBuffer)
     */
    public void dumpAttributesInXML(StringBuffer sb) {
        if (urls.size() == 0) {
            sb.append(">");
            return;
        }
        // XXX vpapad: fishy... we really want to set id
        sb.append(" var='").append(variable.getName());
        sb.append("'/>");
    }

    /**
     * @see niagara.utils.SerializableToXML#dumpChildrenInXML(StringBuffer)
     */
    public void dumpChildrenInXML(StringBuffer sb) {
        ;
    }
}
