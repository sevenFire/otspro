<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    	<meta charset="UTF-8"/>
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>登录--xInsight big-data</title>
		<link rel="shortcut icon" href="jsp/images/icon.ico"/>
		
		<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet">
		<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
		<link href="jsp/css/content-style.css" rel="stylesheet" type="text/css">
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
		<script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
		<script type="text/javascript" src="jsp/js/common.js"></script>
		
		 <style type="text/css">
          	.code   
          	{   
               font-family:Arial;   
               font-style:italic;   
               color:Red;   
               border:0;   
               padding:2px 3px;   
               letter-spacing:3px;   
               font-weight:bolder;   
           	}   
           	.unchanged   
           	{   
               border:0;   
           	}   
	       	body,td,th {
				font-family: "Microsoft YaHei", "宋体", Tahoma, Helvetica, Arial;
			}
			body {
				background-color: #EFEFEF;
			}
         </style>   
</head>

<body class="loginBody"  onload="createCode()">
	 <div class="loginboxbg">
       <div  class="loginlogo"></div>
       <div  class="logincontent">
       <div class="leftpic"> <img src="jsp/images/loginpic.png" width=524 height=335></div>
          <div class="rightcontent">
             <div class="logintitle"  style="height:50px;font-size:24px;margin-top:15px;color:#004d6f;">
             <!-- <img src="jsp/images/logintitle.png" > hm-->
             登&nbsp;&nbsp;&nbsp;&nbsp;录
             </div>
           <div class="logincontent_group">
                <div class="logintext" style="width:80px">用&nbsp;&nbsp;&nbsp;&nbsp;户：</div>
                <div class="l_textbox"><input name="username" id="username" type="text" class="logintextbox" style="width:260px"></div>
           </div>
           <div class="logincontent_group">
                <div class="logintext" style="width:80px">密&nbsp;&nbsp;&nbsp;&nbsp;码：</div>
                <div class="l_textbox"><input name="password" id="password" type="password" class="logintextbox" style="width:260px"></div>
           </div>
           <!-- 
            <div class="logincontent_group">
                <div class="logintext" style="width:80px">验证码：</div>
                <div class="l_textbox"><input id="Verificationcode" name="Verificationcode" type="text" class="logintextbox" style="width:180px;">
				<input type="text" onclick="createCode()" readonly="readonly" id="checkCode" style="width:75px;height:35px;font-size:16px;BACKGROUND-IMAGE: url(jsp/images/verify_code.png);"></div>
           </div>
            -->
          <div class="logincontent_group">
                <div class="logintext"  style="width:80px"></div>
                <div class="l_textbox">
                  <!-- <div class="logintext_left"  style="width:20px"><input id="autologin" name="autologin" type="checkbox" value="" ></div>
                  <div class="logintext_left"  style="width:120px">一周内自动登录</div>
                   <div class="f_pwbox"  style="width:120px"><a href="#" class="f_pw">忘记密码？</a></div>
                   -->
                   <div class="r_text14" id="loginAlert"></div>
                </div>
           </div>
           <div class="logincontent_group">
                <div class="logintext" style="width:80px"></div>
                <div class="l_textbox"><input name="" type="button" class="loginbtn" style="width:260px" value="登  录" onclick="login();"></div>
           </div>
           <div class="logincontent_group">
                <div class="logintext" style="width:80px"></div>
                <!-- <div class="f_regtext"  style="width:250px"><a href="register.jsp" class="f_reg">免费注册</a></div> -->
           </div>

         </div>
       </div>
    </div>
<!---bottom--->
	<div class="navbar navbar-default navbar-fixed-bottom" role="navigation" id="bottomNav" style="min-height:40px;">
	    <div class="copyright">
	    	<!-- <div class="cy_text" >上海宝信软件股份有限公司©版权所有&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;技术支持：13917562774，&nbsp;baoyuansong@baosight.com</div> -->
	    </div>
    </div>
</body>
 
<script src="jsp/js/MD5.js"></script>
<script>
	function login() { 
		var inUsername = document.getElementById("username").value;
		var password = document.getElementById("password").value;
		var username = inUsername;
		var tenantname = username;
		if (inUsername == null || inUsername.length <= 0 || password == null || password.length <= 0) {
			//check if the input is legal
			$("#loginAlert").html("");
			$("#loginAlert").append("用户名和密码不可以为空!");
			return false;
		}               
	 	//用户名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
	 	var domainPos = String(inUsername).indexOf("@");
	 	if(domainPos >= 0)
	 	{	
	 		username = inUsername.substring(0, domainPos);
	 		tenantname = inUsername.substring(domainPos + 1);	 	
	 	} else
	 	{
			$("#loginAlert").html("");
    		$("#loginAlert").append("用户名错误，缺少租户信息（user@tenant）。");
			return;
	 	}	 		 	

		if(!isValidName(username) || !isValidName(tenantname))
		{
			$("#loginAlert").html("");
			$("#loginAlert").append("用户名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
			return;
		}
		
		/****
		var inputCode = document.getElementById("Verificationcode").value;
		if(inputCode.length <=0)   
		{   
			$("#loginAlert").html("");
			$("#loginAlert").append("请输入验证码！");  
			return false;
		}   
		else if(inputCode.toLowerCase() != code.toLowerCase()) 
		{   
			$("#loginAlert").html("");
			$("#loginAlert").append("验证码输入错误！");   
			createCode();//刷新验证码   
			return false;
		}
		****/
		
		/* password=MD5(password); */		
		$.ajax({
			type: "POST",
			url: "/otscfgsvr/dashboard/login", //event handler url
			data: "{\"tenant\":\"" + escape(tenantname) + "\",\"username\":\"" + escape(username) + "\",\"password\":\"" + password + "\"}",//发送ajax请求
			dataType: "json",
			timeout: 3000,
			success: function(msg) {
				window.location = 'table.jsp';
			},
	   		error: function(msg) { 
	   		   	if (msg["status"] == 401) {
	   		   		$("#loginAlert").html("");
					$("#loginAlert").append("登录失败，用户名或密码错误"); 		
	   		   	}   
	   		   	else {
	   		   	    var retMsg = msg["status"] + " - " + msg["statusText"];
	   		   	    $("#loginAlert").html("");
				    $("#loginAlert").append(retMsg); 
	   		   	}
			}
		});		
	}

	var code;
	function createCode()
	{    
		code = "";
		var codeLength = 4;
		var checkCode = document.getElementById("checkCode");
		var selectChar = new Array(0,1,2,3,4,5,6,7,8,9,'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z');   

		for(var i=0;i<codeLength;i++)
		{ 
			var charIndex = Math.floor(Math.random()*36);
			code +=selectChar[charIndex];
		}
   
        if(checkCode)
        {   
        	checkCode.className="code";   
        	checkCode.value = code;   
        }
    }

	document.onkeydown = function (e) {
		var event = e || window.event;//消除浏览器差异  
		if (event.keyCode == 13) {
			login();
		}
	};
</script>
</html>
