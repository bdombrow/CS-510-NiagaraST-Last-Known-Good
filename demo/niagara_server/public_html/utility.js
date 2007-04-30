
function XPath(node, xpath) {
  var doc = node;
  if (node.ownerDocument != null) {
    doc = node.ownerDocument;
  }
  var answers = doc.evaluate(xpath, node, null, XPathResult.ANY_TYPE, null);
  return answers;
}

function XPathFirst(node, xpath) {
  var answers = XPath(node, xpath);
  return answers.iterateNext();
}

function getdom(str) {
  var parser = new DOMParser();
  var doc = parser.parseFromString(str, "text/xml");
  return doc
}

function getxml(doc) {
  var xmlSerializer = new XMLSerializer();
  xml = xmlSerializer.serializeToString(doc);
  return xml;
}

function getprettyxml(doc) {
  var text = getxml(doc);
  var xml = text.replace(/</,"&lt;");  
  return xml;
}

function showxml(doc) {
  var xmlSerializer = new XMLSerializer();
  xml = xmlSerializer.serializeToString(doc);
  alert(xml);
}

function nodeAsDocument(node) {
  var xml = getxml(node);
  var prolog = "<?xml version='1.0'?><!DOCTYPE plan SYSTEM 'queryplan.dtd'>";
  var docstring = prolog+xml;
  return getdom(docstring);
}

// ------- Communications --------------

function xmlHttpPost(strURL, content, callback, multipart) {
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

    if (multipart==true) { req.multipart = true; }
    req.open('POST', strURL, true);    
    req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');    
    req.send(content);
}