var ROOT_URL = "http://localhost:8020/servlet/httpclient";

// Comment out the next line to add debugging to console if firebug is installed
//var console = dummyConsole();

var query = (<r><![CDATA[
	<?xml version='1.0'?>
	<!DOCTYPE plan SYSTEM 'queryplan.dtd'>
	<plan top="cons">
	<collectInstrumentation id='instr' plan='q1'
	 operators="DBThread2,PhysicalPunctQC1" period="1000"/>
	<construct id="cons" input="instr">
	<cdata><property id=$name>$value</property></cdata>
	</construct>
	</plan> 
	]]></r>).toString();

var stageXML;

function submit_query() {
    var content = 'type=' + encodeURIComponent("execute_qp_query") + "&" 
	          + 'query=' + encodeURIComponent(make_query(query)); 
    sendRequest(content);
}

function make_query(qstr) {
	qstr = qstr.replace("<cdata>", "<![CDATA[", "g");
	qstr = qstr.replace("</cdata>", "]]>", "g");
	return qstr;
}

function parse_response(text) {
	// workaround for mozilla bug 336551
	var response = text.replace("<?xml version='1.0'?>", ""); 
	response = new XML(response);
	if (response.@responseType == "server_query_id") {
		set_status("Connected");
	} else if (response.@responseType == "end_result") {
		set_status("Query ended -- restarting");
		setTimeout(submit_query, 1000);
	} else if (response.@responseType == "query_result") {
		set_status("Processing");		
		var kind = response.responseData.property.@id;
		if (kind == "stage") {
			stageXML = response.responseData.property.value.stage;
		} else {
			stageXML.@now = response.responseData.property.value;
			console.log(stageXML);
			load_stage(stageXML);
		}
	}
}
