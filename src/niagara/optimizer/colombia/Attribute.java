/* $Id: Attribute.java,v 1.1 2002/10/24 01:38:06 vpapad Exp $ */
package niagara.optimizer.colombia;

/** Tuple attributes */
public interface Attribute {
    String getName();
    Domain getDomain();
    Attribute copy();
} 
