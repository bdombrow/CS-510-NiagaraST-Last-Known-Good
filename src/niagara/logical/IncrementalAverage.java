/* $Id */
package niagara.logical;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

public class IncrementalAverage extends IncrementalGroup {
    private Attribute avgAttribute;

    public IncrementalAverage() {
    }

    public IncrementalAverage(
        skolem skolemAttributes,
        Attribute avgAttribute) {
        super(skolemAttributes);
        this.avgAttribute = avgAttribute;
    }

    public void setAvgAttribute(Attribute avgAttribute) {
        this.avgAttribute = avgAttribute;
    }

    public Attribute getAvgAttribute() {
        return avgAttribute;
    }

    public void dump() {
        System.out.println(getName());
    }

    public Op copy() {
        return new IncrementalAverage(skolemAttributes, avgAttribute);
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof IncrementalAverage))
            return false;
        if (o.getClass() != IncrementalAverage.class)
            return o.equals(this);
        IncrementalAverage ia = (IncrementalAverage) o;
        return skolemAttributes.equals(ia.skolemAttributes)
            && avgAttribute.equals(ia.avgAttribute);
    }

    public int hashCode() {
        return skolemAttributes.hashCode() ^ avgAttribute.hashCode();
    }
}
