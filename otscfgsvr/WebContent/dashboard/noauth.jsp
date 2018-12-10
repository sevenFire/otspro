<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>no auth page--xInsight big-data</title>
<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
<link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
<link rel="shortcut icon" href="jsp/images/icon.ico" />
<script type="text/javascript" src="jsp/js/init-js.js"></script>
<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>

<script type="text/javascript">
	function noauth() {
		
		if (getUrlParam("forward") != null)			
			$("#forward").attr("href", getUrlParam("forward"));
		
		//3秒后的定时任务
		window.setTimeout("togoto()", 3000);
	}

	function togoto() {
		if (getUrlParam("forward") != null)
			window.parent.location.href = getUrlParam("forward");
		else
			window.parent.location.href = "framehome.jsp";
	}
</script>
</head>

<body onLoad="noauth()">
	<div
		style="background: url(jsp/images/error.png) center no-repeat; weight: 604; height: 501; padding-top: 15px; padding-bottom: 15px;" align="center">
		<div style="margin-top: 200; margin-bottom: 100;margin-left: 100;">
			<p  style="margin-top:60; margin-bottom: 20;" align="center">
				<br/>
				<font color="#0066ff" size="2">正在跳转，请稍等</font><font color="#0066ff" size="2" face="Arial">...</font>
			</p>
			<font size="1">如果您的浏览器不支持跳转,</font><a id="forward" style="text-decoration: none" href="framehome.jsp"><font size="1" color="#FF0000">请点这里</font></a>.
		</div>
	</div>
</body>
</html>