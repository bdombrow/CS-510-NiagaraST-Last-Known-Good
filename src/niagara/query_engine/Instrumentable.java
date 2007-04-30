package niagara.query_engine;

import java.util.ArrayList;

public interface Instrumentable extends SchemaProducer {
    void getInstrumentationValues(ArrayList<String> instrumentationNames,
            ArrayList<Object> instrumentationValues);

    void setInstrumented(boolean instrumented);
}
