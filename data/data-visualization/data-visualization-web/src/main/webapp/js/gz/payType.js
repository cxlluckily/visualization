var payType = function(opt) {
	
	this.topic = "pay:type:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		if (console) {
			console.log(JSON.stringify(data));
		}
		
		var names=new Array();
		var values=new Array();
		$.each(data,function(name,value) {
			names.push(name);
			var obj={'name':name,'value':value};
			values.push(obj);
		});
		
		var ticketPayment = echarts.init(document.getElementById('mainTicketPayment'));
		optionTicketPayment.legend.data = names;
		optionTicketPayment.series[0].data = values;
		ticketPayment.setOption(optionTicketPayment);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
optionTicketPayment = {
	    tooltip : {
	        trigger: 'item',
	        formatter: "{b} : {c} ({d}%)"
	    },
	    legend: {
	        orient:'vertical',
	        left: '80px',
			top:'100px',
			textStyle:{
		            fontSize:15,
		            color:'#fff'
		    },
	        data:['支付宝','微信','其他']
	    },
	    
	    calculable : true,
	    series : [
	        {
	            type:'pie',
	            color: ['#f67f15','#f3fb09','#10cefe'],
	            radius : [40, 70],
	            center: ['60%', '50%'],
	            roseType : 'radius',
	            labelLine:{
	              normal:{
	                  length:30,
	                  length2:30
	              }  
	            },
	            data:[]
	        },
	        {
	            type:'pie',
	            radius : [80, 81],
	            roseType : 'radius',
	            center: ['60%', '50%'],
	            color: ['#f3fb09'],
	            silent:true,
	            label: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            lableLine: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            data:[
	                {value:100, name:'边1'}
	            ]
	        },
	        {
	            type:'pie',
	            color: ['#87CEEB'],
	            radius : [88, 89],
	            center: ['60%', '50%'],
	            roseType : 'radius',
	            silent:true,
	            label: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            lableLine: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            data:[
	                {value:100, name:'边2'}
	            ]
	        }
	    ]
};