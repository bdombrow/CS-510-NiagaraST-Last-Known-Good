package niagara.physical;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Tunable {
    enum TunableType {
        BOOLEAN,
        INTEGER
    }
    String name();
    TunableType type();
    /** Name of the method that sets this tunable */
    String setter();
    String description();
}
