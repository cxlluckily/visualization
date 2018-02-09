    var myChart = echarts.init(document.getElementById('main'));
     var myChart2 = echarts.init(document.getElementById('main2'));
     var myChart3 = echarts.init(document.getElementById('main3'));

title = '近30日每日售票量';

option = {
    title: {
   
    },
    tooltip: {
        trigger: 'axis'
    },
    toolbox: {
        feature: {
            dataView: {
                show: true,
                readOnly: false
            },
            restore: {
                show: true
            },
            saveAsImage: {
                show: true
            }
        }
    },
    grid: {
        containLabel: true
    },
    legend: {
         data: ['增速','售票量']
    },
          grid:{
            bottom:20,
            left:0.05,
        },
    xAxis: [{
        type: 'category',
        axisTick: {
            alignWithLabel: true
        },
        // data: ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016']
         data: ['1号','2号','3号','4号','5号','6号','7号','8号','9号','10号','11号','12号','13号','14号','15号','16号','17号','18号','19号','20号','21号','22号','23号','24号','25号','26号','27号','28号','29号','30号','31号']
    }],
    yAxis: [{
        type: 'value',
        name: '增速',
        min: 0,
        max: 50,
        position: 'right',
        axisLabel: {
            formatter: '{value} %'
        }
    }, {
        type: 'value',
         name: '售票量',
        min: 0,
        max: 3000,
        position: 'left'
    }],
    series: [{
        name: '增速',
        type: 'line',
        stack: '总量',
            label: {
                normal: {
           
                    show: true,
                    position: 'top',
                }
            },
        lineStyle: {
                normal: {
                    width: 3,
                    shadowColor: 'rgba(0,0,0,0.4)',
                    shadowBlur: 10,
                    shadowOffsetY: 10
                }
            },
            
        // data: [1,13,37,35,15,13,25,21,6,45,32,2,4,13,6,4,11]
         data: [1,13,37,35,15,13,25,21,6,45,32,2,4,13,6,4,11,37,35,15,13,25,21,6,45,32,2,4,13,6,4]
    }, {
         name: '售票量',
        type: 'bar',
        yAxisIndex: 1,
        stack: '总量',
            label: {
                normal: {
                 // 是否显示数字
                    show: false,
                    position: 'top'
                }
            },
                   // 是否更换柱形颜色
                itemStyle:{
            normal:{
                color: "#FF6600" //折线点颜色
            }
        },
        // data: [209,236,325,439,507,576,722,879,938,1364,1806,1851,1931,2198,2349,2460,2735]
               data: [209,236,325,439,507,576,722,879,938,1364,1806,1851,1931,2198,2349,2460,2735,600,2349,938,1364,1806,1851,1931,2198,2349,2460,2735,600,2460,2735]
    }]
};



        // 使用刚指定的配置项和数据显示图表。
        myChart.setOption(option);

// 2  饼形图
        option2 = {
    title : {
        text: '',
        subtext: '',
        x:'center'
    },
    tooltip : {
        trigger: 'item',
        formatter: "{a} <br/>{b} : {c} ({d}%)"
    },
    legend: {
        orient: 'vertical',
        left: '400px',
        data: ['支付宝','微信','盘缠','和包','翼支付','首信','银联APP']
    },

   
    series : [
        {
            name: '访问来源',
            type: 'pie',
            radius : '70%',
            center: ['45%', '40%'],
            data:[
                {value:300, name:'支付宝'},
                {value:34, name:'盘缠'},
                {value:135, name:'和包'},
                {value:48, name:'翼支付'},
                // {value:248, name:'翼支付APP'},
                {value:48, name:'首信'},
                {value:148, name:'银联APP'},
                {value:310, name:'微信'},
                
              
            ],
            itemStyle: {
                emphasis: {
                    shadowBlur: 10,
                    shadowOffsetX: 0,
                    shadowColor: 'rgba(0, 0, 0, 0)'
                }
            }
        }
    ]
};
myChart2.setOption(option2);
// 3折线图


option3 = {
     title: {
        text: '',
    },
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'cross',
            crossStyle: {
                color: '#999'
            }
        }
    },
    toolbox: {
        feature: {
            dataView: {show: true, readOnly: false},
            magicType: {show: true, type: ['line', 'bar']},
            restore: {show: true},
            saveAsImage: {show: true}
        }
    },
    legend: {
        data:['新用户','老用户','新用户情况','老用户情况']
      
    },
      grid:{
            bottom:20,
            right:4,
        },
    xAxis: [
        {
            type: 'category',
            // data: ['8:00','9:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00','18:00','19:00'],
            data: ['1号','2号','3号','4号','5号','6号','7号','8号','9号','10号','11号','12号','13号','14号','15号','16号','17号','18号','19号','20号','21号','22号','23号','24号','25号','26号','27号','28号','29号','30号','31号'],
            axisPointer: {
                type: 'shadow'
            }
        }
    ],
    yAxis: [
        {
            type: 'value',
            name: '人数',
            min: 0,
            max: 3000,
            interval: 500,
            axisLabel: {
                formatter: '{value} '
            }
        },
        {
            type: 'value',
          
            min: 0,
            max: 3000,
            interval: 500,
            axisLabel: {
           
            }
        }
    ],
    series: [
        {
            name:'新用户',
            type:'bar',




            data:[400, 100, 100, 500, 700, 900, 1000, 100, 200, 300, 800, 100,1100,1200,1300,1400,1500,1400,1300,1200,1100,100,1100,1200,1300,1400,1500,1400,1300,1200,1100,],
            // data:[2.0, 4.9, 7.0, 23.2, 25.6, 76.7, 135.6, 162.2, 32.6, 20.0, 6.4, 3.3,2.0, 4.9, 7.0, 23.2, 25.6, 76.7, 135.6, 162.2, 32.6, 20.0, 6.4, 3.3,2.0, 4.9, 7.0, 23.2, 25.6, 76.7, 135.6, 162.2, 32.6, 20.0, 6.4, 3.3]
        },
        {
            name:'老用户',
            type:'bar',


//修改颜色 样式
    itemStyle: {
                    normal: {
　　　　　　　　　　　　　　//好，这里就是重头戏了，定义一个list，然后根据所以取得不同的值，这样就实现了，
                        color: function(params) {
                            // build a color map as your need.
                            var colorList = [
                              // '#FF6633','#FF6633','#FF9999','FF6666'
                              '#FF6633',
                              // 新用户第一个颜色
                            ];
                            return colorList[params.dataIndex]
                        },
　　　　　　　　　　　　　　//以下为是否显示，显示位置和显示格式的设置了
                        label: {
                            // 这样柱状图才不会显示繁密数字,
                            show: false,
                            position: 'top',
//                       
                        }
                    }
                },
                
            data:[100, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100,1200,1300,1400,1500,1400,1300,1200, 900, 1000, 1100,1200,1300,1400,1500,1400,1300,1200,200,100]
            //  data:[2.6, 5.9, 9.0, 26.4, 28.7, 70.7, 175.6, 182.2, 48.7, 18.8, 6.0, 2.3,2.6, 5.9, 9.0, 26.4, 28.7, 70.7, 175.6, 182.2, 48.7, 18.8, 6.0, 2.3,2.6, 5.9, 9.0, 26.4, 28.7, 70.7, 175.6, 182.2, 48.7, 18.8, 6.0, 2.3]
        },
        {
            name:'新用户情况',
            type:'line',
        //       itemStyle:{
        //     normal:{
        //         color: "#0b9bb0" //折线颜色
        //     }
        // },

             itemStyle:{
            normal:{
                color: "#FF9999" //折线点颜色
            }
        },
        lineStyle:{
            normal:{
                // width:10,  //连线粗细
                color: "#33FFCC"  //折线条连线颜色
            }
        },

            yAxisIndex: 1,
             data:[200, 202, 330, 450, 630, 102, 203, 234, 230, 165, 120, 62,200, 220, 330, 450, 630, 102, 203, 234, 230, 165, 120, 602,200, 200, 330, 450, 630, 102, 203, 234, 230]
        },
          
          {
            name:'老用户情况',
            type:'line',
        //       itemStyle:{
        //     normal:{
        //         color: "#0b9bb0" //折线颜色
        //     }
        // },

             itemStyle:{
            normal:{
                color: "#FF9933" //折线点颜色
            }
        },
        lineStyle:{
            normal:{
                // width:10,  //连线粗细
                color: "#33FF33"  //折线条连线颜色
            }
        },

            yAxisIndex: 1,
             data:[420, 382, 433, 545, 663, 1002, 2003, 2304, 2300, 1695, 1200, 692,200, 522, 303, 405, 603, 1002, 203, 234, 230, 165, 120, 862,290, 220, 330, 450, 630, 882, 203, 234, 230]
        },   
            
    ]
};


myChart3.setOption(option3);


