/* $Id */
package niagara.logical;

import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.*;

public class IncrementalAverage extends IncrementalGroup {
    private schemaAttribute avgAttribute;

    public IncrementalAverage() {}
    
    public IncrementalAverage(skolem skolemAttributes, schemaAttribute avgAttribute) {
        super(skolemAttributes);
        this.avgAttribute = avgAttribute;
    }
    
    public void setAvgAttribute(schemaAttribute avgAttribute) {
        this.avgAttribute = avgAttribute;
    }

    public schemaAttribute getAvgAttribute() {
        return avgAttribute;
    }

    public void dump() {System.out.println(getName());}

    public Op copy() {
        return new IncrementalAverage(skolemAttributes, avgAttribute);
    }
    
    public int hashCode() {
        return skolemAttributes.hashCode() ^ avgAttribute.hashCode();
    }
}

