/* $Id: StreamOp.java,v 1.1 2002/12/10 01:21:22 vpapad Exp $ */
package niagara.logical;

import niagara.optimizer.colombia.Attribute;
import niagara.xmlql_parser.op_tree.StreamSpec;

abstract public class StreamOp extends NullaryOp {
    protected StreamSpec streamSpec;
    protected Attribute variable;
    
    public StreamSpec getSpec() {
        return streamSpec;
    }

    public Attribute getVariable() {
        return variable;
    }
    
    public boolean isSourceOp() {
        return true;
    }
}