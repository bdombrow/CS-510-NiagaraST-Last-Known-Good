var activeQueries = new Object();
var rooturl = 'http://localhost:8020/servlet';
var _error = "";
var _measurements;
_physicalplan = "";
var _highlighted;

// XXX vpapad is a big believer in global variables
var sampleListener = null;

NO_PLAN=3;
NO_XML=2;
PLAN_OK=1;
MAXRESULTS=10;
// -------------- User methods ------------------

function compile() {
  // reset
  _physicalplan = null;
  _logicalplan = null;

  form = document.getElementById("cqlsubmit");
  qrys = form.cql.value;
  qrys.replace("\n", "");

  var qrydoc = getdom(qrys);
  
  _logicalplan = qrydoc;
  if (validateLogicalPlan() == PLAN_OK) {
    var type = "prepare_query";
    var qry = makeExecuteQuery();
    sendNiagaraQuery(type, qry, handlePhysicalPlan);
  } else {
    var content = 'cqlsubmit=' + encodeURIComponent(qrys);
    xmlHttpPost(rooturl + "/rivulet/compile", content, handleLogicalPlan, false)    
  }
}

function prepare() {
  var type = "prepare_query";
  var qry = makeExecuteQuery();
  if (qry) {
    sendNiagaraQuery(type, qry, handlePhysicalPlan);
  }
}

function stop() {
  _activeQuery.abort();
  var runbutton = document.getElementById("stop");
  runbutton.setAttribute("id","run");
  runbutton.value = "3. Run";
  Behaviour.apply();
}

function run() {
 
  if (physicalPlanDefined()) {
    var type = "execute_prepared_query";  
    var qry = makeExecutePreparedQuery(getActivePlanId());
  } else { 
    var type = "execute_qp_query";
    var qry = makeExecuteQuery();  
  }

  _activeQuery = sendNiagaraQuery(type, qry, handleQueryResult);
  var runbutton = document.getElementById("run");
  runbutton.setAttribute("id","stop");
  runbutton.value = "4. Stop";
  Behaviour.apply();
}

function highlight(op) {
  colorNode(op);
  showTunables(op);
  _highlighted = op;
  //resetMeasurements();  
}

function monitor() {
  var type = "execute_qp_query";
  var planid = getActivePlanId();  
  var opstring = "";
    
  var ops = XPath(_physicalplan, "//plan/*/@id");
  var opid = ops.iterateNext();
  while (opid) { 
    opstring = opstring + "," + opid.nodeValue; 
    opid = ops.iterateNext(); 
  }
    
  qry = makeInstrumentationQuery(planid, opstring);
  sendNiagaraQuery(type, qry, handleMonitorResult);
}

function tune() {
  var tunables = XPath(document, "//*[@class='tunable']");
  var tunable = tunables.iterateNext();
  while (tunable) {
    var planid = getActivePlanId();
    var opid = _highlighted.firstChild.firstChild.nodeValue;
    var name = tunable.getAttribute("name");   
    var value = tunable.lastChild.value;
    sendNiagaraQuery("set_tunable", planid + "." + opid + "." + name + "=" + value, handleTuneResponse);
    tunable = tunables.iterateNext();    
  }
}

// -------- Response Handlers ------------------

function handleLogicalPlan(logplandoc) {
  if (checkCompileError(logplandoc) != PLAN_OK) {
    showerror();
  } else { 
    _logicalplan = logplandoc;
    svgify(_logicalplan);
  }
}

function handleQueryResult(result) {
  parseNiagaraQueryResult(initializeResultStream, showResult, finalizeResultStream, result);
}

function handleMonitorResult(result) {
  parseNiagaraQueryResult(initializeMonitorStream, showSample, finalizeMonitorStream, result);
}

function handlePhysicalPlan(physplan) {
  var ops = physplan.getElementsByTagName("plan");
  if (ops.length > 0) {
    _physicalplan = physplan;  
    svgify(physplan);
  } else {
    var err = XPathFirst(physplan, "//responseData");
    error(err.firstChild.nodeValue);
  }
}

function handleTuneResponse(response) {
  parseNiagaraQueryResult(doNothing, doNothing, acknowledgeTune, response);
}

function handleTunables(response) {
  parseNiagaraQueryResult(initializeTunableStream, showTunable, finalizeTunableStream, response);
}


// --------- Utility -------------------

function checkCompileError(plandoc) {
  result = plandoc.evaluate('//error', plandoc, null, XPathResult.ANY_TYPE, null);
  err = result.iterateNext();

  if (err != null) {
    seterror(getxml(err.firstChild));  
    return NO_PLAN;
  }
  
  return PLAN_OK;
}

function error(msg) {
  var messages = document.getElementById("messages");
  var msgnode = document.createTextNode(msg);
  messages.appendChild(msgnode);
  messages.innerHTML = messages.innerHTML + "<br/>";
}

function seterror(msg) {
  _error = msg;
}

function message(msg) {
  error(msg);
}

function showerror() {
  error("Compile Error: " + _error);
}

function validateLogicalPlan() {

  if (typeof(_logicalplan) == "undefined") {
    seterror('No logical plan prepared.    (typeof(_logicalplan)=="undefined")');
    return NO_XML;
  }

  result = _logicalplan.evaluate('//plan', _logicalplan, null, XPathResult.ANY_TYPE, null);
  pnodes = _logicalplan.getElementsByTagName("plan");
  plan = result.iterateNext();

  if (plan == null) {
    seterror('No plan node found in logical plan XML');  
    return NO_PLAN;
  }
  
  return PLAN_OK;
}

function makeExecuteQuery() {

  if (validateLogicalPlan() != PLAN_OK) {
    showerror();
    return;
  }
  
  plan = XPathFirst(_logicalplan,'//plan');
  xml = getxml(plan);

  var prolog = "<?xml version='1.0'?><!DOCTYPE plan SYSTEM 'queryplan.dtd'>";
  var qry = prolog+xml;
  
  return qry
}


// ---------  Drawing ----------------

function renderPlan(plan) {
  var planbox = document.getElementById("planbox");
  var i=0;
  for (i=planbox.childNodes.length-1; i>=0; i--) {
    planbox.removeChild(planbox.childNodes[i]);
  }
  // XXX vpapad: I need to do this because dot on my system
  // seems to generate font-size properties without units
  // (which according to firefox 1.5 are invalid for CSS)
  // and then text nodes get the default 1em size, which is too large
  var newPlan = plan.childNodes[2];
  newPlan.setAttribute("style", "font-size: 0.8em;");
  
  planbox.appendChild(plan.childNodes[2]);
  Behaviour.apply(); 
}

function svgify(plan) {
  // plan -> dot
  var ops = plan.evaluate('//plan/*', plan, null, XPathResult.ANY_TYPE, null);

  var header = 'digraph Plan { size="4,6"; ranksep=.3; ';
  var footer = '}';
  var body = ''
 
  var op = ops.iterateNext();

  if (op == null) {
    //error("Physical plan malformed.  '//plan/*' returned null.");
    // no plan, do nothing
    return;
  }

  edgeattributes = " [dir=back, style=bold] ";
  while (op) {
    inputs = op.getAttribute('input');
    id = op.getAttribute("id");
    lbl = op.tagName;
    
    style = 'shape=box, style=filled, color="0.7 0.3 1.0" ';
    data = ' label=' + lbl;
    attributes = ' [' + style + data + ']';

    body = body + id + attributes;
    if (inputs != null) {
      nodes = inputs.split(" ");
      for (var i=0; i<nodes.length; i++) {
        body = body + id + " -> " + nodes[i] + edgeattributes;
      }
    }
    op = ops.iterateNext();
  }

  g = header + body + footer
  //g = 'digraph G { a -> b [dir=back] b -> c [dir=back] c -> d [dir=back]}';
  var url = rooturl+"/rivulet/drawgraph";
  var content = "dot=" + encodeURIComponent(g)
  xmlHttpPost(url, content, renderPlan, false);
}

//-------- Callbacks ----------------
// these need to be organized into javascript "classes"

function doNothing(data) {
}

function acknowledgeTune(data) {
  message("plan tuned");
}


function resetMeasurements() {
   var ms = document.getElementById("measurements"); 
   ms.innerHTML = "measurments for " + _highlighted.firstChild.firstChild.nodeValue + "...<br/>";      
}

function initializeMonitorStream(data) {
   var height = window.screen.availHeight/4;
   var width = window.screen.availWidth/3;   
   var left = window.screen.availWidth/2 - width/2;   
   var top = window.innerHeight - height;
   var attrs = "resizable=yes,scrollbars=yes,status=no,"
   attrs = attrs + "height="+height+",width="+width+",left="+left+",top="+top
   var w = window.open("measurements.xml", "monitor", attrs);
   w.resizeTo(650, 650);   
   message("begin monitor stream");
}

function setSampleListener(sl) {
  sampleListener = sl;
}

function finalizeMonitorStream(data) {
   message("end monitor stream");   
}

function showSample(data) {
  if (sampleListener != null) {
    sampleListener.addSample(data);
  }
}

function initializeResultStream(data) {
   activeQueries[data.serverID] = true;
   message("begin receipt");
   results = document.getElementById("results");
   results.innerHTML = "";
}

function showResult(data) {
  var tuple = XPath(data, "//output/*");
  var attr = tuple.iterateNext();
  var text = "";  
  if (!attr) {
    text = getxml(data);
    var fixed = text.replace(/</,"&lt;");  
  } else {
    while (attr) {
      text = text + attr.tagName + "=" + getxml(attr.firstChild) + ", ";
      attr = tuple.iterateNext();    
    }
    text = text.substr(0, text.length-2);
  }
    
  var datanode = document.createTextNode(text);
  
  results = document.getElementById("results");
  results.appendChild(datanode);
  results.innerHTML = results.innerHTML + "<br/>";  
  if (results.childNodes.length > MAXRESULTS*2) {
    results.removeChild(results.firstChild);
    results.removeChild(results.firstChild);    
  }  
}

function finalizeResultStream(data) {
   stop();
   activeQueries[data.serverID] = false;
   message("end receipt");   
}

function initializeTunableStream(data) {
}

function finalizeTunableStream(data) {
  //We created some controls for the tunables,
  // so reapply behaviours
  Behaviour.apply();
}

function showTunables(op) {
  if (!physicalPlanDefined()) {
    error("Physical Plan required to show tunables.  Try optimize.");
    return;
  }

  var tunables = document.getElementById('tunables');
  var tunableswindow = document.getElementById('tunableswindow');  
  tunables.innerHTML = "";
  tunableswindow.style.visibility="visible";
  tunables.style.visibility="visible";  
  var planid = getActivePlanId();
  var opid = op.firstChild.firstChild.nodeValue;  
  qry = makeShowTunablesQuery(opid, planid);
  sendNiagaraQuery("execute_qp_query", qry, handleTunables);
}

function showTunable(data) {
  var tunables = document.getElementById('tunables');
  var txt = makeWidget(data);
  tunables.appendChild(txt);
  var br = document.createElement("br");    
  tunables.innerHTML = tunables.innerHTML + "<br/>";  
}

function demonstrateBrokenBrowser(op) {
//  <table id='tunablestable'>
//  <tr><td>foo</td><td><input type='textbox' value='value'></input></td></tr>
//  </table>

   var tt = document.getElementById('tunablestable');   
   
   var row = document.createElement("tr");
   var col1 = document.createElement("td");    
   var col2 = document.createElement("td");    
   
   var label = document.createTextNode("foo")
   var control = document.createElement("input");
   control.setAttribute("type", "textbox");
   control.setAttribute("value", "value");

   col1.appendChild(label);
   col2.appendChild(control);
   col2.innerHTML = col2.innerHTML + "<br/>";     
   
   row.appendChild(col1);
   row.appendChild(col2);   
   
   tt.appendChild(row);
   showxml(tt);
}

function setvalue(checkbox) {
  checkbox.setAttribute("value", checkbox.checked);
}

function makeWidget(tunableresponse) {
  var answers = XPath(tunableresponse, '//tunable');
  var tunable = answers.iterateNext();
  if (!tunable) { 
    error("no tunable found in responseData"); 
    return document.createTextNode("")
  } else {
    //return document.createTextNode(getxml(tunable))
    
    var row = document.createElement("div");
    row.setAttribute("class", "tunable");    
    
    var label = document.createTextNode(tunable.getAttribute("name") + ": ")
    row.setAttribute("name", tunable.getAttribute("name"));
    row.setAttribute("xmlns","http://www.w3.org/1999/xhtml");

    var type = tunable.getAttribute("type")
    var value = tunable.getAttribute("value")    
    var control = document.createElement("input");   
    switch (type) {
      case "BOOLEAN": {
        control.setAttribute("type", "checkbox");
        
        // setting the class allows a behaviour to be attached
        // in this case, we set value=checked onclick.
        // so we can just read values independent of the type 
        control.setAttribute("class", "checkbox");        
        
        if (value == "true") {
          control.setAttribute("checked", "");     
        }
        control.setAttribute("value", value);         
      }
      break;
      case "INTEGER": {   
        control.setAttribute("type", "text");
        control.setAttribute("value", value);      
      }
      break;
      default: {
        error("unknown tunable type: " + type);
      }
    }
    
    row.appendChild(label);
    row.appendChild(control);
    
    return row;
  }
}

function onerror(event) {
    message("Error: status=" + event.target.status);
    message("Error: statusText=" + event.target.statusText);
    message("Error: readyState=" + event.target.readyState);
}

function getActivePlanId() {
  var plan = _physicalplan.getElementsByTagName("plan");
  if (plan.length > 0) {
    return plan[0].getAttribute("id");
  } else {
    error("No responseMessage node found in active physical plan:" + getxml(plan) );
  }
}

function physicalPlanDefined() {
  try {
    var xml = getxml(_physicalplan);
  } catch(err) {
    return false;
  }
  return true;
}

function colorNode(op) {
  // XPath won't return svg nodes, sadly.
  
  //showxml(op.parentNode);
  
  // once a node gets highlighted, one will always be for the current plan
  if (_highlighted == null) {
    _originalstyle = op.childNodes[2].getAttribute("style");
  }
    
  var plannodes = op.parentNode.childNodes
  for (x in plannodes) {
    //showxml(plannodes[x]);
    if (plannodes[x].tagName == "g") {
      var box = plannodes[x].childNodes[2];
      box.setAttribute("style", _originalstyle);
    }
  }

  var box = op.childNodes[2];
  box.setAttribute("style", "fill:#1E90FF;stroke:#1E90FF")
}

function currentPlan() {
  if (physicalPlanDefined()) {
    var plan = _physicalplan;
  } else {
    var plan = _logicalplan;  
  }
  return plan;
}

function showdetails(op) {
  tip = document.getElementById('tooltip');
  var title = op.firstChild;
  var plan = currentPlan();
  var xpath = "//*[@id='"+ title.firstChild.nodeValue +"']";
  var realop = XPathFirst(plan, xpath);  
  var tipcontent = document.createTextNode(getxml(realop));
  tip.firstChild.innerHTML = "";
  tip.firstChild.appendChild(tipcontent);
  showToolTip('tooltip');
}

function movedetails(op) {
  positionElement('tooltip');
}

function hidedetails(op) {
  hideToolTip('tooltip');
}

function plotItem(measurement) {
}
