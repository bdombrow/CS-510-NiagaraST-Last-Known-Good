/* $Id: Attribute.java,v 1.2 2002/10/24 03:48:54 vpapad Exp $ */
package niagara.optimizer.colombia;

/** Tuple attributes */
public interface Attribute {
    String getName();
    
    Domain getDomain();
    
    Attribute copy();
} 
