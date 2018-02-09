var Subscribe = function(opt) {
	
	this.topic = "";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		$(this.el).prepend(JSON3.stringify(data) + "<br>");
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}