
function parseNiagaraQueryResult(initialize, callback, finalize, result) {

  var resnodes = result.getElementsByTagName("responseMessage");
  var datas = result.getElementsByTagName("responseData");
  if (resnodes.length > 1) { error("multiple response messages found!"); return;}
  if (datas.length > 1) { error("multiple response data elements found!"); return;}
  
  resnode = resnodes[0];
  data = datas[0];

  switch (resnode.getAttribute("responseType")) {
    case "server_query_id": {
      initialize(data);
    }
    break;
    
    case "query_result": {
      callback(data);      
    }
    break;    
    
    case "end_result": {
      finalize(data);
    }
    break;    

    case "server_error": {
      error("Server error:" + data.firstChild.nodeValue);
    }
    break;  

    case "execution_error": {
      error("execution error:" + data.firstChild.nodeValue);
    }
    break;    
    
    default: {      
      error("bad response type: " + resnode.getAttribute("responseType"));
    }
  }
}

function sendNiagaraQuery(querytype, querytext, callback) {
    // branch for native XMLHttpRequest object
    var content = 'type=' + encodeURIComponent(querytype) + "&" 
	          + 'query=' + encodeURIComponent(querytext); 
	var strURL = rooturl + "/httpclient";
 
    var req;
    // Mozilla/Safari
    if (window.XMLHttpRequest) {
        req = new XMLHttpRequest();
    }
    // IE
    else if (window.ActiveXObject) {
        req = new ActiveXObject("Microsoft.XMLHTTP");
    }
    
    req.onerror = onerror    
    req.onload = function() {
        if (req.readyState == 4) {
            callback(req.responseXML);
        }
    }

    req.multipart = true;
    req.open('POST', strURL, true);    
    req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');    
    req.send(content);
    return req;
   
}

function makeShowTunablesQuery(opid, planid) {
  var prolog = '<?xml version="1.0"?><!DOCTYPE plan SYSTEM "queryplan.dtd">';
  var show  = '<showTunables id="show" planID="planid"></showTunables>';
  var select = '<select id="select" input="show"><pred op="eq"><var value="$operator"/><string value="opid"/></pred></select>';
  var construct = '<construct id="cons" input="select"><![CDATA[<tunable operator=$operator type=$type name=$name description=$description value=$value></tunable>]]></construct>'

  var plan = '<plan top="cons">' + show + select + construct + '</plan>';
  
  var plannode = getdom(plan);

  var planattr = XPathFirst(plannode, "//@planID");
  planattr.nodeValue = planid;
  
  var opattr = XPathFirst(plannode, "//string/@value");
  opattr.nodeValue = opid;
  
  qry = prolog + getxml(plannode);
  return qry;
}

function makeInstrumentationQuery(planid, opstr) {
  var prolog = '<?xml version="1.0"?><!DOCTYPE plan SYSTEM "queryplan.dtd">';
  var collect  = '<collectInstrumentation id="instr" plan="planid" operators=""  period="300"/>';
  var construct = '<construct id="cons" input="instr"><![CDATA[<measurement operator=$operator name=$name time=$time>$value</measurement>]]></construct>'
  var plan = '<plan top="cons">' + collect + construct + '</plan>';
  
  var plannode = getdom(plan);

  var planattr = XPathFirst(plannode, "//@plan");
  planattr.nodeValue = planid;

  var opsattr = XPathFirst(plannode, "//collectInstrumentation/@operators");
  opsattr.nodeValue = opstr;
  
  qry = prolog + getxml(plannode);
  return qry;
}

function makeExecutePreparedQuery(planid) {
  qry = planid;
  return qry;
}
