/* $Id */
package niagara.logical;

import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.*;

public class IncrementalAverage extends IncrementalGroup {
    private schemaAttribute avgAttribute;

    public void setAvgAttribute(schemaAttribute avgAttribute) {
        this.avgAttribute = avgAttribute;
    }

    public schemaAttribute getAvgAttribute() {
        return avgAttribute;
    }

    public void dump() {System.out.println(getName());}
}

