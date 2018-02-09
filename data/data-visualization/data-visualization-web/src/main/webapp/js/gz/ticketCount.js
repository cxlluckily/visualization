var ticketCount = function(opt) {
	
	this.topic = "tickets:30days:4401";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		var chartTicket = echarts.init(document.getElementById('mainTicket'));
		optionTicket.xAxis[0].data = data.TIME;
        optionTicket.series[0].data = data.TICKETS;
        chartTicket.setOption(optionTicket);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
//近30日售票情况
optionTicket = {
    color: ['#3398DB'],
    tooltip : {
        trigger: 'axis',
        axisPointer : {            // 坐标轴指示器，坐标轴触发有效
            type : 'shadow'        // 默认为直线，可选为：'line' | 'shadow'
        }
    },
    grid: {
        left: '0',
        right: '5',
        bottom: '0',
        top:'30',
        containLabel: true
    },
    xAxis : [
        {
            type : 'category',
            // data : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            axisLine: {
                lineStyle: {
                    type: 'solid',
                    color:'#539eb3',
                    width:'2'//坐标轴颜色
                }
            },
            axisLabel: {
                textStyle: {
                    color: '#fff'//坐标轴颜色 //文字颜色
                }
            },
            data:[],
            axisTick: {
                alignWithLabel: true
            }
        }
    ],
    yAxis : [
        {
            type : 'value',
            axisLine: {
                lineStyle: {
                    type: 'solid',
                    color:'#539eb3',
                    width:'2'//坐标轴颜色
                }
            },
            axisLabel: {
                textStyle: {
                    color: '#fff'
                }
            },
        }
    ],
  
    series : [
        {
            name:'',
            type: 'bar',
            itemStyle: {
                normal: {
                    color: new echarts.graphic.LinearGradient(
                        0, 0, 0, 1,
                        [
                            {offset: 0, color: '#83bff6'},
                            {offset: 0.5, color: '#188df0'},
                            {offset: 1, color: '#188df0'}
                        ]
                    )
                },
                emphasis: {
                    color: new echarts.graphic.LinearGradient(
                        0, 0, 0, 1,
                        [
                            {offset: 0, color: '#2378f7'},
                            {offset: 0.7, color: '#2378f7'},
                            {offset: 1, color: '#83bff6'}
                        ]
                    )
                }
            },
            data:[]
        }
    ]

};