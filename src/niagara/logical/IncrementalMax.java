/* $Id */
package niagara.logical;

import niagara.xmlql_parser.op_tree.unryOp;
import niagara.xmlql_parser.syntax_tree.*;

public class IncrementalMax extends IncrementalGroup {
    private schemaAttribute maxAttribute;
    private Double emptyGroupValue;

    public void setMaxAttribute(schemaAttribute maxAttribute) {
        this.maxAttribute = maxAttribute;
    }

    public void setEmptyGroupValue(Double emptyGroupValue) {
	this.emptyGroupValue = emptyGroupValue;
    }

    public Double getEmptyGroupValue() {
	return emptyGroupValue;
    }

    public schemaAttribute getMaxAttribute() {
        return maxAttribute;
    }

    public void dump() {System.out.println(getName());}
}

