<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>license alarm page--xInsight big-data</title>
<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
<link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
<link rel="shortcut icon" href="jsp/images/icon.ico" />
<script type="text/javascript" src="jsp/js/init-js.js"></script>
<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>

<script type="text/javascript">
	function licensealarm() {
		
		if (getUrlParam("forward") != null)			
			$("#forward").attr("href", getUrlParam("forward"));

		var status = "许可证状态错误！ ";
		if (getUrlParam("status") != null) {
			var status_code = getUrlParam("status");
			
			if (status_code > 90000 && status_code <= 90426) {
				var tips = {													
						90001:"提示：许可证不合法(b)",
						90002:"提示：许可证没有打开(b)",
						90003:"提示：信息项不存在(b)",
						90004:"提示：产品代码不合法(b)",
						90005:"提示：许可证文件中不存在匹配的产品代码(b)",
						90006:"提示：错误的许可证检查模式(b)",
						90007:"提示：错误的输入参数(b)",
						90008:"提示：内存不足(b)",
						90010:"提示：无法打开许可证文件(b)",
						90011:"提示：无法读取许可证文件的根节点(b)",
						90012:"提示：错误的密钥格式(b)",
						90013:"提示：错误的签名格式(b)",
						90014:"提示：错误的序列号(b)",
						90015:"提示：许可证尚未开始(b)",
						90016:"提示：许可证已过期(b)",
						90020:"提示：加密狗内的项目名称不匹配(b)",
						90021:"提示：加密狗内的密钥不匹配(b)",
						90022:"提示：主机MAC地址不匹配(b)",
						90023:"提示：获取主机MAC地址失败(b)",
						90024:"提示：没有root权限(b)",
						90025:"提示：Socket操作失败(b)",
						90030:"提示：非授权的操作系统(b)",
						90090:"提示：功能未实现(b)",
						90200:"提示：来自圣天狗的错误码(b)",
						90203:"提示：找不到硬件许可证(b)",
						90426:"提示：找不到指定的硬件许可证键值(b)",
					};
				status = tips[errorcode];
			}
			else {
				if (status_code == 172002)
					status = "许可证授权项信息不完整！";
				else if (status_code == 172003)
					status = "许可证已过期！";
				else if (status_code == 172004)
					status = "功能版本不允许的模块！";
				else if (status_code == 172005)
					status = "许可证功能版本错误！";
				else if (status_code == 172006)
					status = "系统类型错误！";
				else if (status_code == 172007)
					status = "超过许可证授权的数据节点数！";
				else if (status_code == 172008)
					status = "非法的许可证Code值！";
				else if (status_code == 172009)
					status = "许可证未到生效时间！";
				else if (status_code == 172010)
					status = "非法的许可服务器MAC地址！";	
				else if (status_code == 172011)
					status = "未知的服务模块！";		
				else if (status_code == 172012)
					status = "无法读取许可证文件！";
				else if (status_code == -1)
					status = "连接不上许可证服务器或内部错误！";
			}
		}
		$("#status").html(status);

		//5秒后的定时任务
		window.setTimeout("togoto()", 5000);
	}

	function togoto() {
		if (getUrlParam("forward") != null)
			window.parent.location.href = getUrlParam("forward");
		else
			window.parent.location.href = "framelogin.jsp";
	}
</script>
</head>

<body onLoad="licensealarm()">
	<div
		style="background: url(jsp/images/licensealarm.png) center no-repeat; weight: 604; height: 501; padding-top: 15px; padding-bottom: 15px;" align="center">
		<div style="margin-top: 200; margin-bottom: 100;margin-left: 100;">
			<p  style="margin-top:60; margin-bottom: 20;" align="center">
				<br/>
				<font color="#FF0000" size="4"><span id="status">许可证状态错误！ </span></font>
				<br/><br/>
				<font color="#0066ff" size="2">正在跳转，请稍等</font><font color="#0066ff" size="2" face="Arial">...</font>
			</p>
			<font size="1">如果您的浏览器不支持跳转,</font><a id="forward" style="text-decoration: none" href="framelogin.jsp"><font size="1" color="#FF0000">请点这里</font></a>.
		</div>
	</div>
</body>
</html>