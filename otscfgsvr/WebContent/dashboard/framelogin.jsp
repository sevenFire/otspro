<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>frame login--xInsight big-data</title>
<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
<link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
<link rel="shortcut icon" href="jsp/images/icon.ico" />

<script type="text/javascript">
	function framelogin() {
		//0秒后的定时任务
		window.setTimeout("togoto()", 0);
	}

	function togoto() {
		window.parent.location.href = "/xinsightcloud/login.do";
	}
</script>
</head>

<body onLoad="framelogin()">
	<div>
		<div style="margin-top: 200; margin-bottom: 100;margin-left: 100;">
			<p  style="margin-top:60; margin-bottom: 20;" align="center">
				<br/>
				<font color="#0066ff" size="2">正在跳转，请稍等</font><font color="#0066ff" size="2" face="Arial">...</font>
			</p>
			<font size="1">如果您的浏览器不支持跳转,</font><a style="text-decoration: none" href="/xinsightcloud/login.do"><font size="1" color="#FF0000">请点这里</font></a>.
		</div>
	</div>
</body>
</html>