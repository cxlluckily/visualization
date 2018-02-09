var ticketRecord = function(opt) {
	
	this.topic = "ticket-record";
	
	this.pattern = false;
	
	this.handle = function(data) {
		
		var entry = data.entryStation ? data.entryStation : ""; 
		var exit = data.exitStation ? data.exitStation : ""; 
		var stations = (entry ? entry + '~' : "") + (exit ? exit : '');
		var str = '<tr><td>' + data.cityName + '</td><td>' + 
		stations + '</td><td>' + 
		data.ticketType + '</td><td>' + data.ticketNum + '张</td></tr>\
		<tr><td colspan="4" class="ge"></td></tr>';
		
		$("#ticketRecord").prepend(str);
		if($("#ticketRecord tr").length>12){
			$("#ticketRecord tr:gt(11)").remove();
		}
		
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}