package niagara.optimizer.colombia;

public interface ATTR {
    String getName();
    Domain getDomain();
    ATTR copy();
} 
