package niagara.client.qpclient;

import java.util.ArrayList;

public class FirehoseScan extends Operator {
    public FirehoseScan() {
	in = new ArrayList();
	out = new ArrayList();
	type = "firehosescan";
    }
}
