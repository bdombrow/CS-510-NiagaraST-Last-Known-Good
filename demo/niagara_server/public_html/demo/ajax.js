// Event handlers for browser events.
// $Id: ajax.js,v 1.1 2007/05/19 16:57:04 vpapad Exp $

var req;
var timeoutWatch;
var timeout;
var TIMEOUT = 2000;

function armTimeoutWatch() {
	if (timeout != undefined)
		window.clearTimeout(timeout);
	timeoutWatch = Date.now();
	timeout = setTimeout(checkTimeoutWatch, TIMEOUT);
}

function checkTimeoutWatch() {
	console.log(Date.now() - timeoutWatch);
	if (Date.now() - timeoutWatch >= TIMEOUT) {
		error_handler("Connection timed out -- restarting");
	}
	armTimeoutWatch();
}


function sendRequest(content) {
	armTimeoutWatch();
    if (window.XMLHttpRequest) {
    	console.log("Sending request");
		set_status("Connecting");
		req = new XMLHttpRequest();
		req.onload = load_handler;
		req.onerror = error_handler;
		req.multipart = true;
		req.open("POST", ROOT_URL, true);
		req.setRequestHeader('Content-Type',
		                     'application/x-www-form-urlencoded; charset=UTF-8');
		req.send(content);
    } else {
		alert("This page works only on Mozilla browsers");
    }
}

function load_handler(event) {
    if (event.target.readyState == 4) {   
    	armTimeoutWatch();
    	if (req.status == 200) {
	    	parse_response(event.target.responseText);
	    } else {
	    	error_handler("status=" + req.status);
	    }
    }
}

function error_handler(message) {
    console.log("Error occured: %s", message);
    if (req != null)
	    req.abort();
    req = null;
    set_status(message);
    setTimeout(submit_query, 1000);
}

