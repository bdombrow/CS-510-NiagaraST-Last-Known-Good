/* $Id: Domain.java,v 1.2 2003/02/08 02:12:03 vpapad Exp $ */
package niagara.optimizer.colombia;

/**  Domains/Types */
abstract public class Domain {
    protected String name;
    public Domain(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
} 
