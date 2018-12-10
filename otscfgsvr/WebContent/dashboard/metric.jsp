<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<!doctype html>
<html lang="zh-CN">
<head>
    	<meta charset="UTF-8"/>
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">        
	<title>监控--xInsight big-data</title>		
	<link rel="shortcut icon" href="jsp/images/icon.ico"/>
	<link href="jsp/css/echartsHome.css" rel="stylesheet">
	<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
	<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
	<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
	<script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
	<script type="text/javascript" src="jsp/js/init-js.js"></script>
	<script type="text/javascript">
		$(function(){
			showActivePage("metrics");
		});
	
		function getMetricInfo()
		{
			$.ajax({
	 				cache: false,
		    		type: "GET",
		    		url: "/otscfgsvr/api/metrics",
		    		dataType: "json",
		    		timeout: 3000,
		    		success: function(results, msg) {
						var totalReadCount = 0;
						var totalWriteCount = 0;
						var totalRecordCount = 0;
						var totalDiskSize = 0;
						rwCountDistribOption["xAxis"][0]["data"] = [];
						rwCountDistribOption["series"][0]["data"] = [];
						rwCountDistribOption["series"][1]["data"] = [];
						diskSizeOption["xAxis"][0]["data"] = [];
						diskSizeOption["series"][0]["data"] = [];
						recordNumOption["xAxis"][0]["data"] = [];
						recordNumOption["series"][0]["data"] = [];
						if (results["errcode"] == 0)
						{
			    			for (var i = 0; i < results["metric_info_list"].length; i++){
			    				totalReadCount += results["metric_info_list"][i]["read_count"];
								totalWriteCount += results["metric_info_list"][i]["write_count"];
								rwCountDistribOption["xAxis"][0]["data"].push(results["metric_info_list"][i]["table_name"]);
								rwCountDistribOption["series"][0]["data"].push(results["metric_info_list"][i]["read_count"]);
								rwCountDistribOption["series"][1]["data"].push(results["metric_info_list"][i]["write_count"]);
		
								diskSizeOption["xAxis"][0]["data"].push(results["metric_info_list"][i]["table_name"]);
								diskSizeOption["series"][0]["data"].push(results["metric_info_list"][i]["disk_size"]);
								totalDiskSize += results["metric_info_list"][i]["disk_size"];
		
								recordNumOption["xAxis"][0]["data"].push(results["metric_info_list"][i]["table_name"]);
								recordNumOption["series"][0]["data"].push(results["metric_info_list"][i]["region_count"]);
								if (!isNaN(results["metric_info_list"][i]["region_count"]))
								{
									totalRecordCount += results["metric_info_list"][i]["region_count"];
								}
			    			}
							totalRWOption["series"][0]["data"][0]["value"] = totalReadCount;
							totalRWOption["series"][0]["data"][1]["value"] = totalWriteCount;
							
							totalRWOption["legend"]["data"]=[];
							totalRWOption["legend"]["data"].push("读次数");
							totalRWOption["legend"]["data"].push("写次数");
							totalRWOption["legend"]["data"].push("记录总数："+totalRecordCount);
							totalRWOption["legend"]["data"].push("磁盘占用："+totalDiskSize);
		
							totalRWChart.setOption(totalRWOption);
							rwCountDistribChart.setOption(rwCountDistribOption);
							recordNumChart.setOption(recordNumOption);
							diskSizeChart.setOption(diskSizeOption);
							totalRWChart.resize();
							rwCountDistribChart.resize();
							recordNumChart.resize();
							diskSizeChart.resize();
							setTimeout(function (){	getMetricInfo();},60000);
						}
		    		},
	                complete: function(){
	                },
		    		error: function (msg){
		    		}
		    	});
		}
</script>
</head>
 <body>
	<div id="header">
		<jsp:include page="header.jsp" />
	</div>
	<div class="content">
		<div class="container-fluid">
	        <div class="row-fluid example">
				<div id="graphic" style="margin-left:70px;margin-right:70px;margin-top:20px;">
					<div id="totalRWCount" class="main" style='width:49%;float:left;margin-right:0px;padding-right:0;border-right-width:1;height:350px;'>
					</div>
					<div id="rwCountDistrib" class="main" style='width:49%;float:right;margin-left:0px;padding-left:0;border-left-width:1;height:350px;'></div>
				</div>
				<div id="graphic" style="margin-left:70px;margin-right:70px;margin-top:20px;">
					<div id="recordNumDistrib" class="main" style='width:49%;float:left;margin-right:0px;padding-right:0;border-right-width:1;height:350px;'></div>
					<div id="diskSizeDistrib" class="main" style='width:49%;float:right;margin-left:0px;padding-left:0;border-left-width:1;height:350px;'></div>
				</div>
			 </div>
			 <div class="row-fluid"></div>
		</div>
	</div>
	
	<div>
		<jsp:include page="footer.jsp" />
	</div>
	<div> 
		<jsp:include page="errorTips.jsp" />
	</div>
</body>
	<script src="jsp/echarts-2.2.0/build/dist/echarts.js"></script>
	<script type="text/javascript">
        // 路径配置
        require.config({
            paths: {
                echarts: './jsp/echarts-2.2.0/build/dist'
            }
        });
        
        // 使用
        require(
            [
                'echarts',
				'echarts/theme/shine',
                'echarts/chart/bar',
				'echarts/chart/pie' 
            ],
        function (ec,theme){
			// 基于准备好的dom，初始化echarts图表        
			totalRWOption = {
			title : {
				text: '总读写次数',
				subtext: '全表数据',
				x:'center'
			},

			tooltip : {
				trigger: 'item',
				formatter: "{a} <br/>{b} : {c} ({d}%)"
			},
			legend: {
				orient : 'vertical',
				x : 'left',
				data:['读次数','写次数','记录总数','磁盘占用']
			},
			calculable : true,
			series : [
				{
					name:'访问次数',
					type:'pie',
					radius : '55%',
					center: ['50%', 225],
					data:[
						{value:0, name:'读次数'},
						{value:0, name:'写次数'},
					]
				}
			]
			};

		rwCountDistribOption = {
			title : {
				text: '表读写次数',
				subtext: ''
			},
			tooltip : {
				trigger: 'axis'
			},
			legend: {
				data:['读次数','写次数']
			},
			toolbox: {
				show : false,
			},
			calculable : true,
			xAxis : [
				{
					type : 'category',
					data : [0],
					axisLabel: { 
					formatter:function(c){
						for(i in c){ 
						return c.substring(0,14); 
							} 
						} 
					}
				}
			],
			yAxis : [
				{
					type : 'value'
				}
			],
			dataZoom:{
					orient:'horizontal',
					show:true,
					start:0,
					end:100
				},
			series : [
				{
					name:'读次数',
					type:'bar',
					data:[0],
					markPoint : {
						data : [
							{type : 'max', name: '最大值'},
							{type : 'min', name: '最小值'}
						]
					},
					markLine : {
						data : [
							{type : 'average', name: '平均值'}
						]
					}
				},
				{
					name:'写次数',
					type:'bar',
					data:[0,],
					markPoint : {
						data : [
							{name : '最大值', type : 'max'},
							{name : '最小值', type : 'min'}
						]
					},
					markLine : {
						data : [
							{type : 'average', name : '平均值'}
						]
					}
				}
			]
		};

	    diskSizeOption = {
	    	    title : {
	    	        text: '表大小(字节)',
	    	        subtext: ''
	    	    },
	    	    tooltip : {
	    	        trigger: 'axis'
	    	    },
	    	    toolbox: {
	    	        show : false,
	    	    },
	    	    calculable : true,
	    	    yAxis : [
	    	        {
	    	            type : 'value',
	    	        }
	    	    ],
	    	    xAxis : [
	    	        {
	    	            type : 'category',
	    	            data : [0]
	    	        }
	    	    ],
				dataZoom:{
					orient:'horizontal',
						show:true,
						start:0,
						end:100
				},
	    	    series : [
	    	        {
	    	            name:'表大小',
	    	            type:'bar',
	    	            data:[0]
	    	        },
	    	    ]
	    	};
	    	                    

		recordNumOption = {
			    title : {
			        text: '表分区数',
			    },
			    tooltip : {
			        trigger: 'axis'
			    },
			    toolbox: {
			        show : false,
			    },
			    calculable : true,
			    xAxis : [
			        {
			            type : 'category',
			            data : [0]
			        }
			    ],
			    yAxis : [
			        {
			            type : 'value'
			        }
			    ],
				dataZoom:{
					orient:'horizontal',
						show:true,
						start:0,
						end:100
				},
			    series : [
			        {
			            name:'分区数',
			            type:'bar',
			            data:[0],
			            markPoint : {
			                data : [
			                    {type : 'max', name: '最大值'},
			                    {type : 'min', name: '最小值'}
			                ]
			            },
			            markLine : {
			                data : [
			                    {type : 'average', name: '平均值'}
			                ]
			            }
			        },
			    ]
			};
                    
		totalRWChart = ec.init(document.getElementById('totalRWCount'),theme); 
		totalRWChart.setOption(totalRWOption);
		rwCountDistribChart = ec.init(document.getElementById('rwCountDistrib'),theme);
		rwCountDistribChart.setOption(rwCountDistribOption);
		recordNumChart = ec.init(document.getElementById('recordNumDistrib'),theme);
		recordNumChart.setOption(recordNumOption);
		diskSizeChart = ec.init(document.getElementById('diskSizeDistrib'),theme);
		diskSizeChart.setOption(diskSizeOption);
		
		setTimeout(function (){
			getMetricInfo();
			window.onresize = function () { 
                     totalRWChart.resize();  
					 rwCountDistribChart.resize(); 
					 recordNumChart.resize(); 
					 diskSizeChart.resize(); 
                 } 
		},200);
		});
    </script>
</html>
