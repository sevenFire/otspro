<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>诊断页面</title>
	<link rel="shortcut icon" href="dashboard/jsp/images/icon.ico"/>
	<link href="dashboard/jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css" >
	<link href="dashboard/jsp/css/style.css" rel="stylesheet" type="text/css">
	
	<script type="text/javascript" src="dashboard/jsp/js/jquery-1.8.2.min.js"></script>
	<script type="text/javascript" src="dashboard/jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>	
	<script type="text/javascript" src="dashboard/jsp/js/init-js.js"></script>
	<script type="text/javascript" src="dashboard/jsp/js/common.js"></script>	
	<script type="text/javascript">
		$(function(){
			showActivePage("debug");
		});
	</script>
</head>
<body class="body_fix">
	
	<div id="mainDiv" class="content" style="min-height:814px;over-flow-x:hidden">
		<div class="row">
			<div class="row" style="height:40px;">  
				<div class="pagetitle" style="margin-left:20px;margin-top:10px;">当前模块配置</div>
			</div>			
			<div class="row" style="margin:10px;background-color:#FAFAFA;">
				<div class="panel" style="background-color: #FAFAFA; border: 1px solid #C9C9C9; margin: 20px;">
					<div id="config" ></div>
				</div>
			</div>
		</div>
	</div>
	
	<div> 
		<jsp:include page="dashboard/errorTips.jsp" />
	</div>
</body>

<script type="text/javascript">
	$(function() {
		initInfo();
	});

	function initInfo() {
		//document.body.style.cursor="wait"; 
		$.ajax({
			type : "POST",
			url : "/otscfgsvr/servlet/config",
			timeout : 30000,
			success : function(data) {		
				var flag = data.substr(0, 2);
				var realdata = data.substr(2, data.length);
				if (flag == "0,") {
					var configs = new Array();
					configs = realdata.split("##"); //字符分割 
					var content = "";
					var content_log = "";
					for (var i = 0; i < configs.length; i++ ) { 					
						if(configs[i].substr(0, 6) == "log4j.")
							content_log += configs[i] + "<br/>";
						else 
							content += configs[i] + "<br/>"; //分割后的字符输出 
					} 				
					$("#config").html(content + "<br/>" + content_log);
				} else {
					content = realdata;
					$("#config").html(content);
				}
			},
			complete : function() {
				document.body.style.cursor = "default";
			},
			error : function(msg) {
				tableAlertMsg("获取配置信息失败！");
			}
		});
	}

	function tableAlertMsg(msg) {
		errorAlertMsg(msg);
	}
</script>
</html>