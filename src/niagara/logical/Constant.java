/* $Id */
package niagara.logical;

public abstract class Constant implements Atom {
    public abstract String getValue();
    
    public boolean equals(Object other) {
        if (other.toString().equals(getValue()))
            return true;
        return false;
    }
    
    public boolean isConstant() { return true; }
    public boolean isVariable() { return false; }
    public int hashCode() { return getValue().hashCode(); }
}
