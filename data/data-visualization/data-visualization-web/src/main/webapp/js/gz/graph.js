!function () {
  /* 倍率设置. */
  const initScala = 0.1 // 初始缩放倍数.
  const stepScala = 0.1 // 每次放大时,相对于原尺寸的步进倍率.

  /* 原始图片宽高 */
  const oriImgWidth = 1920
  const oriImgHeight = 1080

  /* 基于准备好的dom，初始化echarts实例 */

  /* 更改以下数据,然后调用下 update 方法即可. */
  let currentScala = initScala // 当前显示倍率.

  /* 初始缩放倍率,适应屏幕宽高. */
  const winWidth = window.innerWidth || window.document.documentElement.clientWidth || window.document.body.clientWidth
  const winHeight = window.innerHeight || window.document.documentElement.clientHeight || window.document.body.clientHeight

  let widthScala = winWidth / oriImgWidth
  let heightScala = winHeight / oriImgHeight

  /* 把数组处理为键值对 城市名:[城市坐标] .延迟计算,会在第一次加载时,再计算.*/
  let transDataDict = null

  /* 改用固定宽高.
  如果是自动适配,可以使用代码
  currentScala = widthScala < heightScala ? widthScala : heightScala
   */
  currentScala = 1.0

  /* 车站和人流数据. */
let data = [
    {
        name: '机场南',
        value: [698,28],//3号线
    },{
        name: '高增',
        value: [698,62],
    },{
        name: '人和',
        value:[698,95],
    },{
        name: '龙归',
        value: [697,129],
    },{
       name: '嘉禾望岗',
       value: [698,162],
    },{
       name: '白云大道北',
       value: [720,183],
    },{
       name: '永泰',
       value: [746,210],
    },{
       name: '同和',
       value: [771,236],
    },{
       name: '京溪南方医院',
       value: [800,262],
    },{
       name: '梅花园',
       value: [825,287],
    },{
       name: '燕塘',
       value: [838,319],
    },{
       name: '广州东站',
       value: [838,390],
    },{
       name: '林和西',
       value: [839,435],
    },{
       name: '体育西路',
       value: [839,513],
    },{
       name: '珠江新城',
       value: [839,579],
    },{
       name: '广州塔',
       value: [838,668],
    },{
       name: '客村',
       value: [839,697],
    },{
       name: '大塘',
       value: [838,737],
    },{
       name: '沥滘',
       value: [838,785],
    },{
       name: '厦滘',
       value: [837,834],
    },{
       name: '大石',
       value: [838,882],
    },{
        name:'汉溪长隆',
        value:[839,928],
    },{
       name: '市桥',
       value: [869,986],
    },{
       name: '番禺广场',
       value: [904,1020],
    },{
       name: '黄边',    //2号线
       value: [677,183],
    },{
       name: '江夏',
       value: [650,210],
    },{
       name: '萧岗',
       value: [625,235],
    },{
       name: '白云文化广场',
       value: [598,261],
    },{
       name: '白云公园',
       value: [573,287],
    },{
       name: '飞翔公园',
       value: [561,328],
    },{
       name: '三元里',
       value: [561,363],
    },{
       name: '广州火车站',
       value: [560,395],
    },{
       name: '越秀公园',
       value: [561,431],
    },{
       name: '纪念堂',
       value: [560,470],
    },{
       name: '公园前',
       value: [559,513],
    },{
       name: '海珠广场',
       value: [560,582],
    },{
       name: '市二宫',
       value: [560,621],
    },{
       name: '江南西',
       value: [560,655],
    },{
       name: '昌岗',
       value: [560,696],
    },{
       name: '江泰路',
       value: [561,725],
    },{
       name: '东晓南',
       value: [561,764],
    },{
       name: '南洲',
       value: [561,804],
    },{
       name: '洛溪',
       value: [561,844],
    },{
       name: '南浦',
       value: [560,884],
    },{
       name: '会江',
       value: [560,923],
    },{
       name: '石壁',
       value: [563,964]
    },{
       name: '广州南站',
       value: [563,1000],
    },{
       name: '滘口',//五号线红色
       value: [237,607],
    },{
       name: '坦尾',
       value: [306,538],
    },{
       name: '中山八',
       value: [355,489],
    },{
       name: '西场',
       value: [391,452],
    },{
       name: '西村',
       value: [428,415],
    },{
           name: '小北',
           value: [610,394],
        },{
           name: '淘金',
           value: [659,394],
        },{
           name: '区庄',
           value: [712,394],
        },{
           name: '动物园',
           value: [758,449],
        },{
           name: '杨箕',
           value: [759,513],
        },{
           name: '五羊邨',
           value: [779,579],
        },{
           name: '珠江新城',
           value: [839,579],
        },{
           name: '猎德',
           value: [929,578],
        },{
           name: '潭村',
           value: [967,579],
        },{
           name: '员村',
           value: [1004,579],
        },{
           name: '科韵路',
           value: [1041,578],
        },{
           name: '车陂南',
           value: [1079,579],
        },{
           name: '东圃',
           value: [1141,579],
        },{
           name: '三溪',
           value: [1185,579],
        },{
           name: '鱼珠',
           value: [1230,579],
        },{
           name: '大沙地',
           value: [1278,579],
        },{
           name: '大沙东',
           value: [1318,579],
        },{
           name: '文冲',
           value: [1363,579],
        },{
            name: '香雪',//六号线右开
            value: [1396,268],
        },{
           name: '萝岗',
           value: [1361,269],
        },{
           name: '苏元',
           value: [1329,269],
        },{
           name: '暹岗',
           value: [1294,269],
        },{
           name: '金峰',
           value: [1262,269],
        },{
           name: '黄陂',
           value: [1231,269],
        },{
           name: '高塘石',
           value: [1195,268],
        },{
           name: '柯木塱',
           value: [1157,269],
        },{
           name: '龙洞',
           value: [1121,269],
        },{
           name: '植物园',
           value: [1087,269],
        },{
           name: '长湴',
           value: [1054,297],
        },{
           name: '天河客运站',
           value: [1019,317],
        },{
           name: '天平架',
           value: [788,319],
        },{
           name: '沙河',
           value: [741,319],
        },{
           name: '沙河顶',
           value: [712,344],
        },{
           name: '黄花岗',
           value: [711,369],
        },{
           name: '东山口',
           value: [712,513],
        },{
           name: '东湖',
           value: [691,583],
        },{
           name: '团一大广场',
           value: [648,583],
        },{
           name: '北京路',
           value: [605,583],
        },{
           name: '一德路',
           value: [498,583],
        },{
           name: '文化公园',
           value: [457,583],
        },{
           name: '黄沙',
           value: [411,582],
        },{
           name: '如意坊',
           value: [338,570],
        },{
           name: '河沙',
           value: [248,437],
        },{
           name: '沙贝',
           value: [250,390],
        },{
           name: '横沙',
           value: [249,343],
        },{
           name: '浔峰岗',
           value: [249,297],
        },{
           name: '五山',//三号线剩下一部分
           value: [1020,394],
        },{
           name: '华师',
           value: [1011,472],
        },{
           name: '岗顶',
           value: [983,499],
        },{
           name: '石牌桥',
           value: [937,517],
        },{
           name: '体育中心',//一号线
           value: [939,418],
        },{
           name: '烈士陵园',
           value: [660,513],
        },{
           name: '农讲所',
           value: [611,513],
        },{
           name: '西门口',
           value: [512,513],
        },{
           name: '陈家祠',
           value: [452,513],
        },{
           name: '长寿路',
           value: [411,542],
        },{
           name: '芳村',
           value: [411,624],
        },{
           name: '花地湾',
           value: [411,665],
        },{
           name: '坑口',
           value: [412,707],
        },{
           name: '西朗',
           value: [411,748],
        },{
           name: '万胜围',//8号线
           value: [1079,696],
        },{
           name: '琶洲',
           value: [1031,696],
        },{
           name: '新港东',
           value: [983,696],
        },{
           name: '磨碟沙',
           value: [935,696],
        },{
           name: '赤岗',
           value: [887,696],
        },{
           name: '鹭江',
           value: [769,696],
        },{
           name: '中大',
           value: [700,696],
        },{
           name: '晓港',
           value: [630,697],
        },{
           name: '宝岗大道',
           value: [511,689],
        },{
           name: '沙园',
           value: [488,665],
        },{
           name: '凤凰新村',
           value: [464,642],
        },{
           name: '大学城南',//7号线
           value: [1079,789],
        },{
           name: '板桥',
           value: [1027,894],
        },{
           name: '员岗',
           value: [965,929],
        },{
           name: '南村万博',
           value: [907,929],
        },{
           name: '汉溪长隆',
           value: [839,928],
        },{
           name: '钟村',
           value: [735,929],
        },{
           name: '谢村',
           value: [647,928],
        },{
           name: '石壁',
           value: [563,965],
        },{
           name: '燕岗',//GF线
           value: [491,693],
        },{
           name: '沙涌',
           value: [468,699],
        },{
           name: '鹤洞',
           value: [450,727],
        },{
           name: '菊树',
           value: [371,750],
        },{
           name: '龙溪',
           value: [319,750],
        },{
           name: '金融高新区',
           value: [279,773],
        },{
           name: '千灯湖',
           value: [279,807],
        },{
           name: '𧒽岗',
           value: [280,841],
        },{
           name: '南桂路',
           value: [256,869],
        },{
           name: '桂城',
           value: [211,869],
        },{
           name: '朝安',
           value: [167,869],
        },{
           name: '普君北路',
           value: [124,870],
        },{
           name: '祖庙',
           value: [92,897],
        },{
           name: '同济路',
           value: [92,929],
        },{
           name: '季华园',
           value: [91,960],
        },{
           name: '魁奇路',
           value: [91,991],
        },{
           name: '澜石',
           value: [90,1023],
        },{
           name: '世纪莲',
           value: [139,1050],
        },{
           name: '东平',
           value: [182,1050],
        },{
           name: '新城东',
           value: [226,1050],
        },{
           name: '体育中心南',
           value: [871,460],//线APM
        },{
           name: '天河南',
           value: [872,489],
        },{
           name: '黄埔大道',
           value: [872,533],
        },{
           name: '妇儿中心',
           value: [871,554],
        },{
           name: '花城大道',
           value: [871,594],
        },{
           name: '大剧院',
           value: [871,615],
        },{
           name: '海心沙',
           value: [871,637],
        },{
           name: '黄村',
           value: [1160,498],//四号线
        },{
           name: '车陂',
           value: [1119,537],
        },{
           name: '官洲',
           value: [1079,720],
        },{
           name: '大学城北',
           value: [1079,753],
        },{
            name: '大学城南',
            value: [1079,789],
        },{
           name: '新造',
           value: [1079,819],
        },{
           name: '官桥',
           value: [1079,853],
        },{
           name: '石碁',
           value: [1079,885],
        },{
           name: '海傍',
           value: [1079,918],
        },{
           name: '低涌',
           value: [1079,952],
        },{
           name: '东涌',
           value: [1080,985],
        },{
           name: '庆盛',
           value: [1106,1013],
        },{
           name: '黄阁汽车城',
           value: [1154,1014],
        },{
           name: '黄阁',
           value: [1201,1013],
        },{
           name: '蕉门',
           value: [1250,1013],
        },{
           name: '金洲',
           value: [1298,1013],
        }
]
  let links = []

  /* 处于激活状态的图标. */
  let currentChart = null;
  let currentChartDom = null

  /* 放大. */
  function zoomIn(){
    currentScala += stepScala

    if (currentChartDom) {
      currentChartDom.remove()
    }

    currentChartDom = document.createElement('div')
    currentChartDom.style.left = '0px'
    currentChartDom.style.top = '0px'
    currentChartDom.style.position = 'absolute'
    currentChartDom.style.width = oriImgWidth*currentScala + 'px'
    currentChartDom.style.height = oriImgHeight*currentScala + 'px'

    let parentDom = document.getElementById('subway')
    parentDom.appendChild(currentChartDom)
    currentChart = echarts.init(currentChartDom);

    updateView(currentChart,currentScala, data, links)
  }

  /* 缩小. */
  function zoomOut() {
    currentScala -= stepScala

    if (currentChartDom) {
      currentChartDom.remove()
    }

    currentChartDom = document.createElement('div')
    currentChartDom.style.left = '0px'
    currentChartDom.style.top = '0px'
    currentChartDom.style.position = 'absolute'
    currentChartDom.style.width = oriImgWidth*currentScala + 'px'
    currentChartDom.style.height = oriImgHeight*currentScala + 'px'
    let parentDom = document.getElementById('subway')
    parentDom.appendChild(currentChartDom)
    currentChart = echarts.init(currentChartDom);

    updateView(currentChart,currentScala, data, links)
  }


  /* 记录所有新站点数据. */
  let allLinks = []


  /* 数据更新周期. 秒 */
  const UPDATE_INTERVAL = 1.5

  /* 更新地铁数据. */
  function updateViewData()
  {
    /* 实现对象深拷贝. */
    const allLinksCopy = JSON.parse(JSON.stringify(allLinks))
    allLinks = []

    /* 要先数据去重. */
    let allLinksDict = {}
    let allLinksData = allLinksCopy.reduce((sumArr,linkItem) => {
      /* 实现对象深拷贝. */
      const newItem = JSON.parse(JSON.stringify(linkItem))

      const name = newItem.source
      const toname = newItem.target

      /* 使用起始站点名,来唯一标记数据. */
      const nameId = name + '-' + toname

      if ( ! allLinksDict[nameId]) {
        allLinksDict[nameId] = true
        sumArr.push(newItem)
      }

      return sumArr
    },[])
    updateView(currentChart,currentScala, data, allLinksData)
  }

  /* 更新人流信息的接口.只是实时存,但是更新,是周期性的. */
  function updateLinks(newLinks)
  {
    allLinks = allLinks.concat(newLinks)
  }

  /* 模拟socket调用,在部署时,此函数,请注释掉. */
  function startSocketMoco(callback){
    // /* 每100毫秒回调一次数据. */
    // let i = 0
    //
    // let rtn1 = [{"source":"团一大广场",value:1,"target":"西朗"}]
    // let rtn2 = [{"source":"北京路",value:1,"target":"中大"}]
    //
    //
    // setInterval(()=>{
    //   ++ i
    //   if (i < 3) {
    //     callback(rtn1)
    //   }else{
    //     callback(rtn2)
    //   }
    // }, 1000)

    /* 换成真实的函数调用. */
    Sub.subscribe({
      topic: "ticket-record",
      pattern: false,
      handle: (data) => {
        /* 数据转换. */
        callback(convertData(data))
      }
    })
  }

  /* TODO: 把这一句注释掉,然后在真是的socket请求里,调用:
  updateLinks(data)
   */
  startSocketMoco(updateLinks)

  /* 初始显示. */
  function initShow() {
    /* 当前车站数据和人流信息,要动态从服务器获取. */
    /* 当前车站数据. */
  //     /* 车站人流信息. */
      links = []

      currentChartDom = document.createElement('div')
      currentChartDom.style.left = '0px'
      currentChartDom.style.top = '0px'
      currentChartDom.style.position = 'absolute'
      currentChartDom.style.width = oriImgWidth*currentScala + 'px'
      currentChartDom.style.height = oriImgHeight*currentScala + 'px'
      let parentDom = document.getElementById('subway')
      parentDom.appendChild(currentChartDom)
      currentChart = echarts.init(currentChartDom);
      setInterval(updateViewData, UPDATE_INTERVAL * 1000)
  }

  initShow()

  /* 根据数据或缩放情况,变化视图. */
  function updateView(myChart,currentScala, data, links) {
      /* 图片宽高. */
      const imgWidth = oriImgWidth * currentScala
      const imgHeight = oriImgHeight * currentScala

      /* 数据处理. */
      /* 站点数据,坐标变换. */
      let transData = data.map((item) => {
          /* 实现对象深拷贝. */
          const newItem = JSON.parse(JSON.stringify(item))

          newItem.value[0] = newItem.value[0] * currentScala
      newItem.value[1] = newItem.value[1] * currentScala
      newItem.value[1] = oriImgHeight * currentScala - newItem.value[1]

      return newItem
  }
  )

  /* 把数组处理为键值对 城市名:[城市坐标] */
  if ( ! transDataDict) {
    transDataDict = transData.reduce((sum, item) => {
            /* 实现对象深拷贝. */
            const newItem = JSON.parse(JSON.stringify(item))

            sum[newItem.name] = newItem.value

            return sum
        }, {}
    )
  }

  let transLinks = links.map((linkItem) => {
      /* 实现对象深拷贝. */
      const newItem = JSON.parse(JSON.stringify(linkItem))

      newItem.name = newItem.source
  newItem.toname = newItem.target
  newItem.coords = [transDataDict[newItem.name], transDataDict[newItem.toname]]
  return newItem
  })

  /* 需要加光圈的车站. */
  let topStationsDict = {}
  let topStationsData = links.reduce((sumArr, linkItem) => {
          /* 实现对象深拷贝. */
          const newItem = JSON.parse(JSON.stringify(linkItem))

          const name = newItem.source
          const toname = newItem.target

          if ( !topStationsDict[name])
  {
    topStationsDict[name] = true
    sumArr.push({
        name: name,
        value: transDataDict[name]
    })
  }

  if (!topStationsDict[toname]) {
    topStationsDict[toname] = true
    sumArr.push({
        name: toname,
        value: transDataDict[toname]
    })
  }

  return sumArr
  },
  []
  )

  /* 数据结构,应该联网获取. */
  let option = {
      grid: {
          left: 0,
          top: 0,
          right: 0,
          bottom: 0,
      },
      graphic: {
          type: 'image',
          style: {
              image: '',
              // image: './graph.jpg',
              width: imgWidth,
              height: imgHeight,
          },
          z: -1,
      },
      xAxis: {
          nameGap: 0,
          show: false,
          type: 'value',
          min: 0,
          max: imgWidth
      },
      yAxis: {
          nameGap: 0,
          show: false,
          type: 'value',
          min: 0,
          max: imgHeight,
      },

      series: [{
          zlevel: 1,
          type: 'lines',
          name: 'guangzhou-subway',
          coordinateSystem: 'cartesian2d',
          data: transLinks,
          //线上面的动态特效
          effect: {
              show: true,
              period: UPDATE_INTERVAL*0.9,
              trailLength: 0.01,
              //  飞线颜色
              //  color: '#6beedf',
              color: '#0de3cc',
              symbolSize: 5
          },
          lineStyle: {
              normal: {
                  width: '',
                  color: '#88dcda',
                  curveness: 0.2
              }
          },
          //  markPoint : {
          //      symbol:'circle',
          //      symbolSize : 30,
          //      label: {
          //        normal: {
          //          show: true,
          //          formatter: '{b}',
          //          textStyle: {
          //            color: '#f00'
          //          }
          //        }
          //      },
          //      effect : {
          //          show: false,
          //          shadowBlur : 0
          //      },
          //      data : [
          //          {name:'新城东', coord: [100, 100]},
          //          {name:'东圃',value:60},
          //          {name:'南村万博',value:30},
          //      ]
          //  }
      },
          {
              symbolSize: 10,
              symbol: "circle",
              "name": "Top 5",
              "type": "effectScatter",
              "coordinateSystem": "cartesian2d",

              //
              symbolSize: 13,

              "data": topStationsData,
              "showEffectOn": "render",
              "rippleEffect": {
                  "brushType": "stroke",
                  //换成另一种波纹
                  //
                  //  "brushType": "fill",
                  //  scale: 10,
                  scale: 3,

              },
              "hoverAnimation": true,
              "label": {
                  "normal": {
                      "formatter": "",
                      "position": "right",
                      "show": true
                  }
              },
              "itemStyle": {
                  "normal": {
                      //   圆点颜色  红色
                      //   "color": "#FF00FF",
                      //  #A8D4FF 蓝色
                      //    color: 'orange',
                      color: '#ffb400',
                      "shadowBlur": 10,
                      //   "shadowColor": "#f00",
                      "shadowColor": "#333",
                      opacity: 1
                  }
              },
              "zlevel": 2,
              //  markPoint : {
              //      symbol:'circle',
              //      symbolSize : 30,
              //      label: {
              //        normal: {
              //          show: true,
              //          formatter: '{b}',
              //          textStyle: {
              //            color: '#f00'
              //          }
              //        }
              //      },
              //      effect : {
              //          show: true,
              //          shadowBlur : 0
              //      },
              //      data : [
              //          {name:'新城东', coord: [100, 100]},
              //          {name:'东圃',value:60},
              //          {name:'南村万博',value:30},
              //      ]
              //  }
          }
      ]
  }

  // 使用刚指定的配置项和数据显示图表。
  myChart.setOption(option);
  }

  /* 数据转换. */
  function convertData(data) {
    if ( ! data || ! transDataDict) {
      return []
    }

    if ( ! transDataDict[data.TICKET_PICKUP_STATION_NAME_CN]) {
    //   console.error(data.TICKET_PICKUP_STATION_NAME_CN + '车站坐标数据不存在,请及时补充!')
      return []
    }

    if ( ! transDataDict[data.TICKET_GETOFF_STATION_NAME_CN]) {
    //   console.error(data.TICKET_GETOFF_STATION_NAME_CN + '车站坐标数据不存在,请及时补充!')
      return []
    }

    return [
      {
        source: data.TICKET_PICKUP_STATION_NAME_CN,
        target: data.TICKET_GETOFF_STATION_NAME_CN,
        value: data.TICKET_ACTUAL_TAKE_TICKET_NUM ,
      }
    ]
  }
}()
