/* $Id: Attribute.java,v 1.3 2003/02/08 02:12:03 vpapad Exp $ */
package niagara.optimizer.colombia;

/** Tuple attributes */
public interface Attribute {
    String getName();
    
    Domain getDomain();
    
    Attribute copy();
} 
