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
