
var Ticket = function(opt) {
	
	this.topic = "tickets-*";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		var	className = "in_re_ta in_re_ta2";
		var oldTime = $(".ticket_real_time").text();
		if (data.TICKET_NOTI_TAKE_TICKET_RESULT_DATE > oldTime) {
			$(".ticket_real_time").text(data.TICKET_NOTI_TAKE_TICKET_RESULT_DATE);
		}
		if(data.TICKET_GETOFF_STATION_NAME_CN == '--'){
			data.TICKET_GETOFF_STATION_NAME_CN = '';
		}else{
			data.TICKET_GETOFF_STATION_NAME_CN = '~' + data.TICKET_GETOFF_STATION_NAME_CN ;
		} 
		$("#dataDiv").prepend("<table border='0' cellpadding='0' cellspacing='0' class='" + className +"' style='text-align: center;'>"
		     +"<tr style='text-align: center;'>"
	         +"  <td style='width: 100px;'>" + data.CITY_NAME +"</td>"
	         +"  <td style='width: 300px;'>" + data.TICKET_PICKUP_STATION_NAME_CN +"" + data.TICKET_GETOFF_STATION_NAME_CN + " </td>"
	         +"  <td style='width: 300px;'>" + data.TICKET_ACTUAL_TAKE_TICKET_NUM + "张</td>"
	         +" </tr>"
	         +"</table>");
		if($("#dataDiv table").length>6){
			$("#dataDiv table:gt(5)").remove();
		}
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
