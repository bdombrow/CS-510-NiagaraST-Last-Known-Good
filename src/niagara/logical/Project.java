/* $Id: Project.java,v 1.6 2003/07/03 19:39:02 tufte Exp $ */
package niagara.logical;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.Strings;
import niagara.xmlql_parser.op_tree.unryOp;

/** Project operator (without duplicate elimination) */
public class Project extends unryOp {
    /** attributes to project on */
    Attrs attrs;
   
    public Op opCopy() {
        return new Project(this);
    }

    public Project() {}

    public Project(Attrs attrs) {
        this.attrs = attrs;
    }

    public Project(Project other) {
        attrs = other.attrs.copy();
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Project))
            return false;
        if (other.getClass() != Project.class)
            return other.equals(this);
        return attrs.equals(((Project) other).attrs);
    }
    
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty inputLogProp = input[0];
        assert inputLogProp.Contains(attrs) : "Cannot project on invisible attributes";
        LogicalProperty result = inputLogProp.copy();        
        Attrs oldAttrs = result.getAttrs();
        // Maintain order of input tuple for projected attributes
        Attrs newAttrs = new Attrs(attrs.size());
        for (int i = 0; i < oldAttrs.size(); i++) {
            Attribute a = oldAttrs.get(i);
            if (attrs.Contains(a))
                newAttrs.add(a);
        }
        result.setAttrs(newAttrs);
        return result;
    }
    
    public int hashCode() {
        return attrs.hashCode();
    }

    public Attrs getAttrs() {
        return attrs;
    }
}
