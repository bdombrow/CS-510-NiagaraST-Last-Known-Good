package niagara.client.qpclient;

import diva.graph.*;
import diva.graph.model.*;
import diva.util.*;

import java.util.*;

public class FirehoseScan extends Operator {
    public FirehoseScan() {
	in = new ArrayList();
	out = new ArrayList();
	type = "firehosescan";
    }
}
