

var rules = {
    '#compile' : function(button) {
       button.onclick = compile
     },
    '#prepare' : function(button) {
       button.onclick = prepare
     },
    '#run' : function(button) {
       button.onclick = run
     },      
    '#stop' : function(button) {
       button.onclick = stop
     },        
    '#monitor' : function(button) {
       button.onclick = monitor
     },
    '#tune' : function(button) {
       button.onclick = tune
     },             
    '.checkbox' : function(checkbox) {
       checkbox.onclick = function() {
         setvalue(checkbox);
       }
     },     
    '.node' : function(operator) {
       operator.onclick = function() {
         highlight(operator);
       }
       operator.onmouseover = function() {
         showdetails(operator);
       }
       operator.onmouseout = function() {
         hidedetails(operator);
       }
       operator.onmousemove = function() {
         movedetails(operator);
       }
    }
};

Behaviour.register(rules);
