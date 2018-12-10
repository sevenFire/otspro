<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%> 
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>帮助--OTS REST</title>
<link rel="shortcut icon" href="jsp/images/icon.ico" />
<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
<link href="jsp/css/patch.css" rel="stylesheet">
<link href="jsp/css/docs.min.css" rel="stylesheet">
<link href="jsp/css/style.css" rel="stylesheet" type="text/css">

<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
<script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
<script type="text/javascript" src="jsp/js/ie-emulation-modes-warning.js"></script>
<script type="text/javascript" src="jsp/js/docs.min.js"></script>
<script type="text/javascript" src="jsp/js/init-js.js"></script>
<script type="text/javascript">
	$(function() {
		showActivePage("helper");
		$("#topNav").removeClass("navbar-fixed-top");
		$("#topNav").addClass("navbar-static-top");
	});
</script>

	<script type="text/javascript" src="jsp/code/scripts/shCore.js"></script>
    <!-- 下面 pre 中 brush 是 java，这里就要导入 shBrushJava 的 js 文件 -->
    <!-- scripts 目录下还有很多的 shBrushXxxx.js 文件 -->
    <script type="text/javascript" src="jsp/code/scripts/shBrushJava.js"></script>
    <link type="text/css" rel="stylesheet" href="jsp/code/styles/shCore.css"/>
    <!-- 这里使用 eclipse 外观，styles 目录下还有很多的外观 -->
	<link type="text/css" rel="stylesheet" href="jsp/code/styles/shThemeEclipse.css"/>
	<script type="text/javascript">
		SyntaxHighlighter.all();
	</script>
	
</head>

<body style="overflow: scroll; overflow-x: hidden">

	<div class="container bs-docs-container" style="margin-top: 20px;">
		<div class="row">
			<div class="col-md-3" role="complementary">
				<nav class="bs-docs-sidebar hidden-print hidden-xs hidden-sm affix-top">
					<ul class="nav bs-docs-sidenav">
						<li class="active"><a href="#environment_deploy">环境部署</a>
							<ul class="nav">
								<li><a href="#deploy">部署</a></li>
								<li><a href="#test_tools">测试工具</a></li>
							</ul>
						</li>
						<li><a href="#concept_interpret">概念释疑</a>
							<ul class="nav">
								<li><a href="#rest">Rest</a></li>
								<li><a href="#json">Json</a></li>
								<li><a href="#https">https</a></li>
							</ul>
						</li>
						<li><a href="#resource_identity">资源标识</a></li>
						<li><a href="#state_code">状态码</a></li>
						<li><a href="#security">安全</a>
							<ul class="nav">
								<li><a href="#security_basic_auth">Basic Authentication</a></li>
								<li><a href="#security_token_cert">Token Certification</a></li>
							</ul>
						</li>
						<li><a href="#interface_description">接口描述</a>
							<ul class="nav">
								<li><a href="#auth_operator">权限认证</a>
									<ul class="nav">
										<li><a href="#auth_operator_gettoken">&nbsp;&nbsp;获取token</a></li>
									</ul>
								</li>
								<li><a href="#table_operator">表操作</a>
									<ul class="nav">
										<li><a href="#table_operator_getall">&nbsp;&nbsp;获取所有表</a></li>
										<li><a href="#table_operator_getbyname">&nbsp;&nbsp;按名称查询表</a></li>
										<li><a href="#table_operator_get"> &nbsp;&nbsp;获取表的信息</a></li>
										<li><a href="#table_operator_create"> &nbsp;&nbsp;创建表</a></li>
										<li><a href="#table_operator_update"> &nbsp;&nbsp;更新表</a></li>
										<li><a href="#table_operator_delete"> &nbsp;&nbsp;删除表</a></li>
									</ul>
								</li>
								<li><a href="#data_operator">记录操作</a>
									<ul class="nav">
										<li><a href="#data_operator_get"> &nbsp;&nbsp;读取记录</a></li>
										<li><a href="#data_operator_getbykeys">&nbsp;&nbsp;多键读取记录</a></li>
										<li><a href="#data_operator_insert"> &nbsp;&nbsp;插入记录</a></li>
										<li><a href="#data_operator_update"> &nbsp;&nbsp;更新记录</a></li>
										<li><a href="#data_operator_delete"> &nbsp;&nbsp;删除记录</a></li>
										<li><a href="#data_operator_truncate">&nbsp;&nbsp;清空记录</a></li>
										<!-- <li><a href="#data_operator_file_upload">&nbsp;&nbsp;上传文件</a></li>
										<li><a href="#data_operator_file_get">&nbsp;&nbsp;读取文件</a></li> -->
									</ul>
								</li>
								<li><a href="#index_operator">索引操作</a>
									<ul class="nav">
										<li><a href="#index_operator_getall">&nbsp;&nbsp;获取所有索引</a></li>
										<li><a href="#index_operator_get"> &nbsp;&nbsp;获取索引信息</a></li>
										<li><a href="#index_operator_create">&nbsp;&nbsp;创建索引</a></li>
										<li><a href="#index_operator_update">&nbsp;&nbsp;更新索引</a></li>
										<li><a href="#index_operator_delete">&nbsp;&nbsp;删除索引</a></li>
										<li><a href="#index_operator_queryrec">&nbsp;&nbsp;基于索引查询记录</a></li>
									</ul>
								</li>
								<li><a href="#monitor_operator">监控</a>
									<ul class="nav">
										<li><a href="#monitor_operator_getall">&nbsp;&nbsp;获取所有表的监控信息</a></li>
										<li><a href="#monitor_operator_get">&nbsp;&nbsp;获取单表的监控信息</a></li>
									</ul>
								</li>
							</ul>
						</li>
						<li><a href="#java_example">JAVA示例</a></li>
						<li><a href="#error_code">错误码</a>
							<ul class="nav">
								<li><a href="#ots_error_code">OTS REST操作</a></li>
								<li><a href="#table_error_code">表操作（Hbase）</a></li>
								<li><a href="#index_error_code">基于Solr索引操作</a></li>			    	
						    	<li><a href="#index_hbase_error_code">基于Hbase索引操作</a></li>
								<!-- <li><a href="#configure_error_code">配置操作</a></li> -->
							</ul>
						</li>
						<li><a href="#appendix">附录</a>
							<ul class="nav">
								<li><a href="#appendix_namerule">命名规则</a></li>
							</ul>
						</li>
						<li><a href="#faq">FAQ</a></li>
					</ul>
					<a class="back-to-top" href="#top">返回顶部</a>
				</nav>
			</div>
			<div class="col-md-9" role="main">
				<div class="bs-docs-section">
					<h1 id="environment_deploy" class="page-header">环境部署</h1>
					<h3 id="deploy">部署</h3>
					<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
						<div class="row">大数据存储平台OTS REST服务目前部署在宝信云服务上，对外提供HTTP访问服务。目前仅支持JSON交换协议。</div>
					</div>
					<h3 id="test_tools">测试工具</h3>
					<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
						<ul>
							<li><a href="http://baike.baidu.com/view/1326315.htm">curl(linux自带工具)</a></li>
							<li><a href="http://www.getpostman.com/docs">postman(chrome插件)</a></li>
							<li><a href="http://www.jb51.net/softs/43863.html">httpdebug</a></li>
							<li><a href="https://pypi.python.org/pypi/pycurl/">pycurl</a></li>
						</ul>
					</div>
				</div>

				<div class="bs-docs-section">
					<h1 id="concept_interpret" class="page-header">概念释疑</h1>
					<h3 id="rest">Rest</h3>
					<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
						<div class="row">"表述性状态传递"（REpresentational State Transfer，REST）于2000年由Roy Fielding提出，是一个基于HTTP的设计架构。常见操作：</div>
						<div class="c_bg">
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td>GET</td>
									<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;从服务器取出资源（一项或多项）。</td>
								</tr>
								<tr style="height: 30px;">
									<td>POST</td>
									<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在服务器新建一个资源。</td>
								<tr style="height: 30px;">
									<td>PUT</td>
									<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在服务器更新资源（客户端提供改变后的完整资源）。</td>
								</tr>
								<tr style="height: 30px;">
									<td>PATCH</td>
									<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;在服务器更新资源（客户端提供改变的属性）。</td>
								</tr>
								<tr style="height: 30px;">
									<td>DELETE</td>
									<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;从服务器删除资源。</td>
								</tr>
							</table>
						</div>
						<div class="row" style="margin-top: 10px; margin-bottom: 10px;">OTS REST提供GET/POST/PUT/DELETE命令，分别用于获取资源、新建资源、更新资源、删除资源。</div>
					</div>
					<h3 id="json">Json</h3>
					<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
							<div class="row" >参见&nbsp;<a href="http://www.w3school.com.cn/json/">json教程</a></div>
						</div>
				        <h3 id="https">https</h3>
					<div style="margin-top:20px;margin-left:20px;margin-right:20px;margin-bottom:20px;">
						<div class="row" >参见&nbsp;<a href="http://zh.wikipedia.org/zh/%E8%B6%85%E6%96%87%E6%9C%AC%E4%BC%A0%E8%BE%93%E5%AE%89%E5%85%A8%E5%8D%8F%E8%AE%AE">wiki百科-超文本传输安全协议</a></div>
					</div>
		        	</div>
		        

				<div class="bs-docs-section">
					<h1 id="resource_identity" class="page-header">资源标识</h1>
		          		<div style="margin-top:20px;margin-left:20px;margin-right:20px;margin-bottom:20px;">
		   				<div class="row" style="margin-top:20px;margin-bottom:10px;">OTS REST调用的URL语法如下：
							<p><code style="font-size: 18px;">http://example.domain.com/otsrest/api/<span	style="color: #007130; font-weight: bold;">path</span>/<span style="color: #e60013; font-weight: bold;">resource</span>?<span style="color: #1f497d; font-weight: bold;">argument=value</span></code></p>
							<p>其中：</p>
							<ul>
								<li>path指代不同的资源，标识对不同的资源进行操作（如下表所示）</li>
								<li>resource标识具体的资源内容</li>
								<li>argument和value用于设置资源满足的条件</li>
								<li>*注：example.domain.com为示例，可以换成实际的IP或者域名!</li>
							</ul>
					  		<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">URL前缀</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;含义</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">http://example.domain.com/otsrest/api/<b>token</b></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;权限认证</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">http://example.domain.com/otsrest/api/<b>table</b></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;表操作</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">http://example.domain.com/otsrest/api/<b>record</b></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;记录操作</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">http://example.domain.com/otsrest/api/<b>index</b></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;索引操作</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">http://example.domain.com/otsrest/api/<b>metrics</b></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;&nbsp;&nbsp;监控操作</td>
								</tr>
							</table>
		   			  		<div class="row" style="margin-top:10px;margin-bottom:10px;">上述URL中resource、argument、value依据不同的目的需要具体地实例化，详见<a href="#interface_description"><span style="color:#2d3ebe">"接口描述（JSON语法）"</span></a>说明举例。</div>
						</div>
					</div>
				</div>
				<div class="bs-docs-section">
					<h1 id="state_code" class="page-header">状态码</h1>
					<div class="row" style="margin-top: 20px;">服务器向用户返回的状态码和提示信息，常见的有以下一些（方括号中是该状态码对应的HTTP动词）。</div>
					<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
						<tr style="height: 30px;">
							<td width="50" bgcolor="#F6F6F6">Code</td>
							<td width="200" bgcolor="#F6F6F6">&nbsp;&nbsp;Status</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">200</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Ok</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[GET/DELETE]：服务器成功返回用户请求的数据，该操作是幂等的</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">201</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Created</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[POST/PUT/PATCH]：用户新建或修改数据成功</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">202</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Accepted</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*]：表示一个请求已经进入后台排队（异步任务）</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">204</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;No Content</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[DELETE]：用户删除数据成功，无需返回信息</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">400</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Bad Request</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*]：用户发出的请求有错误。可能语义有误，当前请求无法被服务器理解，除非进行修改，否则客户端不应该重复提交这个请求；或者请求参数有误。服务器没有进行操作</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">401</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Unauthorized</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*]：表示用户没有权限（令牌、用户名、密码错误）</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">403</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Forbidden</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*] 表示用户得到授权（与401错误相对），但是访问是被禁止的</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">404</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Not Found</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*]：用户发出的请求针对的是不存在的记录，服务器没有进行操作</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">406</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Not Acceptable</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[GET]：用户请求的格式不可得（比如用户请求JSON格式，但是只有XML格式）</td>
						</tr>
						<tr style="height: 30px;">
							<td bgcolor="#F6F6F6">500</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;Internal Server Error</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;[*]：服务器发生错误，用户将无法判断发出的请求是否成功</td>
						</tr>
					</table>
					<div class="row" style="margin-top:20px;">更详细的状态码信息参见&nbsp;<a href="https://www.addedbytes.com/articles/for-beginners/http-status-codes">HTTP Status Codes</a></div>
		        </div>
		
		        		<div class="bs-docs-section">
		        			<h1 id="security" class="page-header">安全</h1>
		        	
		        			<h3 id="security_basic_auth">Basic Authentication</h3>		        	
		          			<div style="margin-top:20px;margin-left:20px;margin-right:20px;margin-bottom:20px;">
		   					<div class="row" style="margin-top:20px;margin-bottom:10px;">OTS REST采用HTTP Basic Authentication认证方式作为<span style="font-weight:bold">基础权限认证</span>，即HTTP请求头部包含Authorization字段，例如：</div>
		    				<div class="c_bg" >Authorization: "Basic aHVhbmdAZG9tYWluOmh1YW5n"</div>
		   					<div class="row" style="margin-top:10px;margin-bottom:10px;">其中，aHVhbmdAZG9tYWluOmh1YW5n<span style="font-weight:bold">为用户名和密码的<span style="color:#F00; border:medium;">base64</span>加密字符串，</span>在OTS REST中，用户名要求采用以下格式： </div>
		    				<div class="c_bg" >UserName@TenantName</div>		    			
		    				<div class="row" style="margin-top:10px;margin-bottom:10px;">注意：&nbsp;&nbsp;
		    					<span style="font-weight:bold">UserName</span>和
		    					<span style="font-weight:bold">TenantName</span>均要求满足：
		    					<span style="color:#F00; border:medium;">只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。</span>		    				
		    				
		    				</div>
		    				<div class="row" style="margin-top:10px;margin-bottom:10px;">HTTP Basic Authentication介绍：</div>
							<ul> 
								<li>
									<a href="http://en.wikipedia.org/wiki/Basic_access_authentication" style="font-weight:normal;">http://en.wikipedia.org/wiki/Basic_access_authentication</a>
								</li>
								<li>
									<a href="http://tools.ietf.org/html/rfc2617" style="font-weight:normal;">http://tools.ietf.org/html/rfc2617</a>
								</li>
							</ul>
						</div>

						<br />
						<h3 id="security_token_cert">Token Certification</h3>
		          			<div style="margin-top:20px;margin-left:20px;margin-right:20px;margin-bottom:20px;">
		   					<div class="row" style="margin-top:20px;margin-bottom:10px;">为一些应用场景的应用方便，OTS REST同时支持Token认证的请求方式，有两种使用模式：</div> 
		   					<div class="row" style="margin-top:20px;margin-bottom:10px;">1、获取有效token后可在URL请求中包含<span style="color:#F00; border:medium;font-weight:bold">&amp;token=&lt;token&gt;</span>参数，URL方式举例：</div> 
		    				<div class="c_bg" >http://example.domain.com/otsrest/api/table/_all_tables?<span style="font-weight:bold">token=7df7b07a56844a7ab44c48e2821b6759</span></div>
		   					<div class="row" style="margin-top:10px;margin-bottom:10px;">其中，7df7b07a56844a7ab44c48e2821b6759权限认证接口获取到的用户身份认证token，（详见<a href="#auth_operator_gettoken"><span style="color:#2d3ebe; font-weight:bold;">"获取token"</span></a>接口）。 </div>
  			
		   					<div class="row" style="margin-top:20px;margin-bottom:10px;">2、类似头部HTTP Basic Authentication的使用方式，在Http头部增加token字段，举例如下：</div> 
							<div class="c_bg">token: "7df7b07a56844a7ab44c48e2821b6759"</div>
					</div>

					<div class="bs-docs-section">
						<h1 id="interface_description" class="page-header">接口描述</h1>

						<h3 id="auth_operator">权限认证</h3>
						<!-- <a href="jsp/code/testSample.zip">权限认证信息获取示例代码</a> -->
		       	  			<div style="margin-top:20px;margin-left:20px;margin-right:20px;margin-bottom:20px;">
		   					<div class="row" style="margin-top:20px;">权限认证的URL前缀：http://example.domain.com/otsrest/api/<B>token</B></div>
		   					<br/>
		   					<p id="auth_operator_gettoken" style="font-weight:bold;"> 获取token</p>
							<div class="c_bg">
								<p><code>GET /</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"token":"281f8768b98f493d93535e44bc30e54f",&nbsp;&nbsp;//token </code></p>
								<p><code>"errcode": 0 &nbsp;&nbsp; //错误码 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">获取用户的认证token信息。</div>
							<div class="row" style="margin-top: 20px;">举例（均以curl为例，下同）：-i参数用于显示交互信息</div>
							<div class="c_bg">
								<p><code> % curl -u username@tenant:password -i	http://example.domain.com/otsrest/api/token </code></p>
								<p><code></code></p>
								<p><code>HTTP/1.1 200 OK</code></p>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<p><code></code></p>
								<p><code>{</code></p>
								<p><code>"token":"281f8768b98f493d93535e44bc30e54f",</code></p>
								<p><code>"errcode": 0</code></p>
								<p><code>}</code></p>
							</div>
						</div>
						<div class="row" style="margin-top:20px;font-weight:bold">特别说明：<span style="color:#F00; border:medium;">本接口必须使用basic认证方式，</span>后续接口可采用token认证方式，使用本接口获得的token。</div>

						<br />
						<h3 id="table_operator">表操作</h3>
						<!-- <a href="jsp/code/testSample.zip">表操作示例代码</a> -->
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
		   					<div class="row" style="margin-top:20px;">表操作的URL前缀：http://example.domain.com/otsrest/api/<B>table</B></div>
							<br />
							<p id="table_operator_getall" style="font-weight: bold;">获取所有表</p>
							<div class="c_bg">
								<p><code>GET /_all_tables</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;//错误码 </code></p>
								<p><code>"total_count":2, &nbsp;&nbsp;//满足条件的表的总个数 </code></p>
								<p><code>"table_names": &nbsp;&nbsp; //此次返回的表名列表 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baokang"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">返回用户的所有表，包括表的数目和表名的列表。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<p><code> % curl -u username@tenant:password -i http://example.domain.com/otsrest/api/table/_all_tables </code></p>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/table/_all_tables?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<p><code></code></p>
								<p><code>HTTP/1.1 200 OK</code></p>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<p><code></code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0,</code></p>
								<p><code>"total_count":4,</code></p>
								<p><code>"table_names":</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baokang",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baoing",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baoxin"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>

							<p id="table_operator_getbyname" style="font-weight: bold;">按名称查询表</p>
							<div class="c_bg">
								<p><code>GET ?name=subname[&amp;limit=num1[&amp;offset=num2]]</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;//错误码 </code></p>
								<p><code>"total_count":4, &nbsp;&nbsp;//满足条件的表的总个数 </code></p>
								<p><code>"table_names": &nbsp;&nbsp; //此次返回的表名列表 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baokang"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">返回满足名称的表，包括满足条件的表的总数目和此次返回的表名的列表。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<p><code> % curl -u username@tenant:password -i http://example.domain.com/otsrest/api/table?name=in&amp;limit=2&amp;offset=0</code></p>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/table?name=in&amp;limit=2&amp;offset=0&amp;token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<p><code></code></p>
								<p><code>HTTP/1.1 200 OK</code></p>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<p><code></code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0,</code></p>
								<p><code>"total_count":4,</code></p>
								<p><code>"table_names":</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baoing",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baoxin"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="table_operator_get" style="font-weight: bold;">获取表的信息</p>
							<div class="c_bg">
								<p><code>GET &nbsp; /&lt;tablename&gt;</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0,</code></p>
								<p><code>"table_name":"baosight", &nbsp;&nbsp;//表名 </code></p>
								<p><code>"description":"baosight test table", &nbsp;&nbsp;//表的描述信息 </code></p>
								<p><code>"compression_type":1, &nbsp;&nbsp; //压缩算法:0-none,1-snappy,2-lz4,3-gz,4-lzo</code></p>
								<p><code>"primary_key_type":1, &nbsp;&nbsp; //主键类型: 0-hash key, 1-hash and range key</code></p>
								<p><code>"hash_key_type":1, &nbsp;&nbsp; //hashkey类型:0-string,1-number,2-binary</code></p>
								<p><code>"range_key_type":1, &nbsp;&nbsp; //rangekey类型:	0-string,1-number,2-binary</code></p>
								<p><code>"create_time":"2015-09-16 15:12:02", &nbsp;&nbsp;//创建时间</code></p>
								<p><code>"modify_time":"2015-09-16 15:12:02" &nbsp;&nbsp;//最后修改时间</code></p>
								<!--<p><code>"mob_enabled":true, &nbsp;&nbsp; //是否支持mob</code></p>
								<p><code>"mob_threshold":100 &nbsp;&nbsp; //mob阈值</code></p>-->
								<p><code>}</code></p>
							</div>
			   				<div class="row" style="margin-top:20px;">返回表&lt;tablename&gt;的属性信息，包含描述信息、压缩算法、表示表的状态等。
								<span style="color:#F00; border:medium;">表名&lt;tablename&gt;只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。</span>
							</div>
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="205" bgcolor="#F6F6F6">字段</td>
									<td bgcolor="#F6F6F6">含义</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">table_name</td>
									<td bgcolor="#F6F6F6">表名</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">description</td>
									<td bgcolor="#F6F6F6">描述信息</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">compression_type</td>
									<td bgcolor="#F6F6F6">压缩算法，0-none,1-snappy,2-lz4,3-gz,4-lzo(暂无)</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">primary_key_type</td>
									<td bgcolor="#F6F6F6">主键类型: 0-hash key, 1-hash and range key</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">hash_key_type</td>
									<td bgcolor="#F6F6F6">hashkey类型:0-string,1-number,2-binary(通信时使用hex编码)</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">range_key_type</td>
									<td bgcolor="#F6F6F6">rangekey类型:0-string,1-number,2-binary(通信时使用hex编码)</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">create_time</td>
									<td bgcolor="#F6F6F6">创建时间</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">modify_time</td>
									<td bgcolor="#F6F6F6">最后修改时间</td>
								</tr>
								<!--<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">mob_enabled</td>
									<td bgcolor="#F6F6F6">是否支持mob，默认false</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">mob_threshold</td>
									<td bgcolor="#F6F6F6">mob阈值，默认100（102400字节）</td>
								</tr>-->
							</table>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/table/baosight</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/table/baosight?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<p><code></code></p>
								<P><code>HTTP/1.1 200 OK</code></P>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<p><code></code></p>
								<P><code>{</code></P>
								<p><code>"errcode":0,</code></p>
								<p><code>"table_name":"baosight", </code></p>
								<p><code>"description":"baosight test table", </code></p>
								<p><code>"compression_type":1,</code></p>
								<p><code>"primary_key_type":1,</code></p>
								<p><code>"hash_key_type":1,</code></p>
								<p><code>"range_key_type":1, </code></p>
								<p><code>"create_time":"2015-09-16 15:12:02",</code></p>
								<p><code>"modify_time":"2015-09-16 15:12:02"</code></p>
								<!--<p><code>"mob_enabled":true,</code></p>
								<p><code>"mob_threshold":100</code></p>-->
								<P><code>} </code></P>
							</div>

							<br /> 
							<br />
							<p id="table_operator_create" style="font-weight: bold;">创建表</p>
							<div class="c_bg">
								<p><code>POST &nbsp; /&lt;tablename&gt;</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"primary_key_type":1, &nbsp;&nbsp; //主键类型: 0-hash key, 1-hash and range key</code></p>
								<p><code>"hash_key_type":1, &nbsp;&nbsp; //hashkey类型:0-string,1-number,2-binary</code></p>
								<p><code>"range_key_type":1, &nbsp;&nbsp; //rangekey类型:	0-string,1-number,2-binary</code></p>
								<p><code>"description":"baosight test table", &nbsp;&nbsp;//表的描述信息 </code></p>
								<p><code>"compression_type":1 &nbsp;&nbsp; //压缩算法:0-none,1-snappy,2-lz4,3-gz,4-lzo</code></p>
								<!--<p><code>"mob_enabled":true, &nbsp;&nbsp; //支持mob，可选项 </code></p>
								<p><code>"mob_threshold":100 &nbsp;&nbsp; //mob阈值，可选项 </code></p>-->
								<p><code>}</code></p>
								<p><code></code></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">创建表时可以按需要指定表的一些属性，上述属性为属性全集，且均为可选项。当不指定任何属性时仍然需要确保POST的消息体是个合法的JSON语法串。创建成功时HTTP返回状态码是201。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/table/baosight -d '{"primary_key_type":1, "hash_key_type":1, "range_key_type":1, "description":"baosight", "compression_type":0}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/table/baosight?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"primary_key_type":1, "hash_key_type":1, "range_key_type":1, "description":"baosight", "compression_type":0}'</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 201 CREATED</code></P>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top:20px;">举例说明：<span style="color:#F00; border:medium;">需要指定数据类型"Content-Type:application/json"，curl命令中-d选项后数据由单引号包含，特别地，window环境下curl命令-d后数据不需要单引号且数据中双引号需要使用转义字符。后续类似，不再重复。</span></div>

							<br /> 
							<br />
							<p id="table_operator_update" style="font-weight: bold;">更新表</p>
							<div class="c_bg">
								<p><code>PUT &nbsp; /&lt;tablename&gt;</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"description":"table description"	&nbsp;&nbsp;//描述信息，可选项 </code></p>
								<!--<p><code>"mob_enabled":true, &nbsp;&nbsp;//支持mob，true支持,可选项 </code></p>
								<p><code>"mob_threshold":100 &nbsp;&nbsp; //mob阈值，可选项 </code></p>-->
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">更新表时可以按需要指定表的一些属性，上述属性为属性全集，且均为可选项。当不指定任何属性时仍然需要确保PUT的消息体是个合法的JSON语法串。更新成功时HTTP返回状态码是201，具体操作的错误码以返回值为准。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/table/baosight -d '{"description":"baosight_new"}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/table/baosight?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"description":"baosight_new"}'</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 201 CREATED </code></P>
								<P><code>Cache-Control: no-cache </code></P>
								<P><code>Content-Type: application/json </code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp; //区分不同操作的结果</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="table_operator_delete" style="font-weight: bold;">删除表</p>
							<div class="c_bg">
								<p><code>DELETE &nbsp; /&lt;tablename&gt;</code></p>
								<p><code></code></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">删除表成功时HTTP返回状态码是200，具体操作的错误码以返回值为准。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X DELETE -i http://example.domain.com/otsrest/api/table/baosight</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X DELETE -i http://example.domain.com/otsrest/api/table/baosight?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 200 OK</code></P>
								<P><code>Cache-Control: no-cache</code></P>
								<P><code>Content-Type: application/json</code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0 </code></p>
								<p><code>}</code></p>
							</div>
						</div>

						<br />
						<h3 id="data_operator">记录操作</h3>
						<!-- <a href="jsp/code/testSample.zip">记录操作示例代码</a> -->
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
							<div class="row" style="margin-top: 20px;">记录操作的URL前缀：http://example.domain.com/otsrest/api/<B>record</B></div>
							<br />
							<p id="data_operator_get" style="font-weight: bold;">读取记录</p>
							<div class="c_bg">
								<!-- 
									<p><code>GET&nbsp;/&lt;tablename&gt;?hash_key=hashkey[&amp;range_key=rangekey][[&amp;range_key_start=start][&amp;range_key_end=end]][&amp;range_key_prefix=prefix][&amp;columns=column1,..columnN][&amp;limit=10][[&amp;range_key_cursor_mark=0][&amp;offset=0]][&amp;descending=false] </code></p>
							 	-->
								<p><code>GET &nbsp;/&lt;tablename&gt;?hash_key=hashkey[&amp;range_key=rangekey][[&amp;range_key_start=start][&amp;range_key_end=end]][&amp;range_key_prefix=prefix][&amp;columns=column1,..columnN][&amp;limit=10][[&amp;range_key_cursor_mark=*][&amp;offset=0]]</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;</code></p>
								<p><code>"range_key_next_cursor_mark":xtdndlfsgadercwxkdx, &nbsp;&nbsp;//16进制编码(无前缀无空格)的游标,仅当使用range_key_cursor_mark时存在 </code></p>
								<p><code>"records" : [</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang" : "abc1"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 10px; margin-bottom: 10px;">读取记录时（仅支持一次操作一张表），可以按需指定相应的查询配置项，即[]部分是可选的(下文同)，具体含义如下：</div>
							<table style="width='550'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="180" bgcolor="#F6F6F6">字段</td>
									<td width="200" bgcolor="#F6F6F6">含义</td>
									<td width="120" bgcolor="#F6F6F6">&nbsp;&nbsp;备注</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">hash_key=hashkey</td>
									<!-- 
							  			<td rowspan="4" bgcolor="#F6F6F6" style="word-break:break-all;">查询方式包括精确查询、匹配查询和范围查询三种。精确查询hash_key=hashkey&amp;range_key=rangekey通过指定精确的rowkey值查询相应记录，匹配查询时，以M开头的查询表达为hash_key=hashkey&amp;range_key_prefix=M。范围查询默认是左闭右开区间，即hash_key=hashkey&amp;range_key_start=1000&amp;range_key_end=2000表示范围[1000,2000)，即查询结果不含2000，范围查询同时可以指定range_key_prefix的匹配规则，返回范围内满足匹配要求的记录。</td>
							  		-->
									<td rowspan="4" bgcolor="#F6F6F6" style="word-break: break-all;">查询方式包括精确查询、匹配查询和范围查询三种。不带任何key的查询参数时查询所有记录。精确查询hash_key=hashkey&amp;range_key=rangekey通过指定精确的rowkey值查询相应记录，匹配查询时，以M开头的查询表达为hash_key=hashkey&amp;range_key_prefix=M。范围查询默认是左闭右开区间，即hash_key=hashkey&amp;range_key_start=1000&amp;range_key_end=2000表示范围[1000,2000)，即查询结果不含2000，范围查询同时可以指定range_key_prefix的匹配规则，返回范围内满足匹配要求的记录。</td>
									<td rowspan="4" bgcolor="#F6F6F6">&nbsp;&nbsp;四选一，必有项</td>
								</tr>
								<tr style="height: 80px;">
									<td bgcolor="#F6F6F6">hash_key=hashkey&amp;range_key=rangekey</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">hash_key=hashkey&amp;range_key_prefix=prefix</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">hash_key=hashkey[&amp;range_key_start=start][&amp;range_key_end=end][&amp;range_key_prefix=prefix]</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">columns=column1,column2,column3</td>
									<td bgcolor="#F6F6F6">当返回记录时，指定记录仅返回包含列的集合</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">limit=100&amp;offset=0</td>
									<td bgcolor="#F6F6F6">默认迭代方式，限制返回，用于分页功能。默认limit为100，最大值10000，offset默认为0</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">limit=100&amp;range_key_cursor_mark=*</td>
									<td bgcolor="#F6F6F6">限制返回，用于分页功能。默认limit为100，最大值10000，range_key_cursor_mark开始时为*，当返回的range_key_next_cursor_mark值为*时表示已返回所有满足条件的记录。</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<!-- 
								<tr style="height:30px;">
								  	<td bgcolor="#F6F6F6">descending=false</td>
								  	<td bgcolor="#F6F6F6">返回记录的排序，默认descending为false</td>
								  	<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
							 	-->
							</table>
							<div class="row" style="margin-top:20px;"><span style="color:#F00; border:medium;">提示：记录中列名构成的推荐规则为：只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。如果使用其他字符会导致使用统计计算服务时判断列名为非法！</span></div> 
							<div class="row" style="margin-top: 20px;">举例（匹配查询）：</div>
							<div class="c_bg">
								<p><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;limit=10</code></p>
								<p><code>或者</code></p>
								<p><code> % curl -i	http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;limit=10&amp;token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;</code></p>
								<p><code>"records"&nbsp;:&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" :	"10001", &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
								<P><code></code></P>
							</div>

							<div class="row" style="margin-top: 20px;">举例（精确查询）：</div>
							<div class="c_bg">
								<p><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;range_key=10002</code></p>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;range_key=10002&amp;token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;</code></p>
								<p><code>"records":&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"huang" : "abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" :	"10001", &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="data_operator_getbykeys" style="font-weight: bold;">多键读取记录</p>
							<div class="c_bg">
								<p><code>POST &nbsp; /&lt;tablename&gt;/keys</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"key_list" : [ </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; { </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "range_key" : "10001"&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; },</code></p>
								<p><code>&nbsp;&nbsp;&nbsp; { </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "range_key" : "10002"&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; }</code></p>
								<p><code>&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"records": [</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang":"abc2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang":"abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
			   				<div class="row" style="margin-top:20px;">多键值查询记录时（仅支持一次操作一张表），<span style="color:#F00; border:medium;">使用POST将多个键值的条件传入，属于精确查询。</span></div>  
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight/keys -d '{"key_list":[{"hash_key":"10001","range_key":"10001"}, {"hash_key":"10001","range_key":"10002"}]}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight/keys?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"key_list":[{"hash_key":"10001","range_key":"10001"}, {"hash_key":"10001","range_key":"10002"}]}'</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 200 OK </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<p><code>Content-Type: application/json </code></P>
								<p><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp;&nbsp;</code></p>
								<p><code>"records"&nbsp;:&nbsp;[</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" :	"10001", &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" :	"10001", &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
								<P><code></code></P>
							</div>

							<br /> 
							<br />
							<p id="data_operator_insert" style="font-weight: bold;">插入记录</p>
							<div class="c_bg">
								<p><code>POST &nbsp; /&lt;tablename&gt;</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"records" : [ </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; { </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "huang" : "abc3",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "range_key" : "10001"&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; }</code></p>
								<p><code>&nbsp;&nbsp; ]</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp; //区分操作的结果是否可信</code></p>
								<p><code>}</code></p>
							</div>
			   				<div class="row" style="margin-top:20px;">插入记录时（仅支持一次操作一张表），<span style="color:#F00; border:medium;">每个record必须包含"hash_key"项，这是OTS REST服务默认rowkey列之一，如果rowkey类型包含range_key，则也必须包含"range_key"。</span></div>  
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight -d '{"records":[{"hash_key":"10001","range_key":"10001","huang":"abc3"}]}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"records":[{"hash_key":"10001","range_key":"10001","huang":"abc3"}]}'</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 201 CREATED </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<p><code>Content-Type: application/json </code></P>
								<p><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="data_operator_update" style="font-weight: bold;">更新记录</p>
							<div class="c_bg">
								<p><code>PUT &nbsp; /&lt;tablename&gt;</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"records" : [ </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; { </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "huang" : "abc3",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; "range_key" : "10001"&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp; }</code></p>
								<p><code>&nbsp;&nbsp; ]</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp; //区分操作的结果是否可信</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top:20px;">更新记录时（仅支持一次操作一张表），<span style="color:#F00; border:medium;">每个record必须包含"hash_key"项，这是OTS REST服务默认rowkey列之一，如果rowkey类型包含range_key，则也必须包含"range_key"。</span></div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight -d '{"records":[{"hash_key":"10001","range_key":"10001","huang":"abc3_update"}]}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/record/baosight?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"records":[{"hash_key":"10001","range_key":"10001","huang":"abc3_update"}]}'</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 201 CREATED </code></P>
								<P><code>Cache-Control: no-cache </code></P>
								<P><code>Content-Type: application/json </code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="data_operator_delete" style="font-weight: bold;">删除记录</p>
							<div class="c_bg">
								<p><code>DELETE &nbsp; /&lt;tablename&gt;?hash_key=hashkey[&amp;range_key=rangekey][[&amp;range_key_start=start][&amp;range_key_end=end]][&amp;range_key_prefix=prefix]</code></p>
								<p><code>或者</code></p>
								<p><code>DELETE &nbsp; /&lt;tablename&gt;?end_time=endtime[&amp;start_time=starttime] &nbsp;//按策略删除，time格式：yyyy-MM-dd HH:mm:ss:S</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>&nbsp; "errcode":0&nbsp;//区分操作的结果是否可信 </code></p>
								<p><code>}</code></p>
							</div>
			  				<div class="row" style="margin-top:20px;">删除记录时（仅支持一次操作一张表），可以按需指定相应的查询配置项，具体含义参见查询记录说明，<span style="color:#F00; border:medium;">注意，按策略删除只有参数end_time（必须项）和start_time（可选项）</span>。</div>						
							<div class="row" style="margin-top: 20px;">删除成功时HTTP返回状态码是200，具体操作的错误码以返回值为准。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X DELETE -i http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;range_key=10001</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X DELETE -i http://example.domain.com/otsrest/api/record/baosight?hash_key=10001&amp;range_key=10001&amp;token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<P><code>Content-Type: application/json </code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="data_operator_truncate" style="font-weight: bold;">清空记录</p>
							<div class="c_bg">
								<p><code>DELETE &nbsp; /&lt;tablename&gt;/truncate</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>&nbsp; "errcode":0&nbsp;//区分操作的结果是否可信 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">清空记录将删除所有记录。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X DELETE -i http://example.domain.com/otsrest/api/record/baosight/truncate</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X DELETE -i http://example.domain.com/otsrest/api/record/baosight/truncate?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<P><code>Content-Type: application/json </code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
							</div>

							<!--<br /> 
							<br />
							<p id="data_operator_file_upload" style="font-weight: bold;">上传文件</p>
							<div class="c_bg">
								<p><code>POST &nbsp; /file/&lt;tablename&gt;?hash_key=hashkey[&amp;range_key=rangekey]&amp;column=column_mob</code></p>
								<p></p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>&nbsp; &lt;实际的文件流&gt;//实际的文件流 </code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>&nbsp; "errcode":0&nbsp;//区分操作的结果是否可信 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">上传文件时（仅支持一次操作一张表），需要制定具体的列名，具体含义如下：</div>
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="405" bgcolor="#F6F6F6">字段</td>
									<td width="121" bgcolor="#F6F6F6">含义</td>
									<td width="120" bgcolor="#F6F6F6">备注</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">hash_key=hashkey[&amp;range_key=rangekey]</td>
									<td bgcolor="#F6F6F6">行键值，hash_key必有，range_key视其表的行健类型而定</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;必有项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">column=column_mob</td>
									<td bgcolor="#F6F6F6">文件存储的列名</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;必有项</td>
								</tr>
							</table>
							<div class="row" style="margin-top:20px;"><span style="color:#F00; border:medium;">特别注意：此处的头部的指定类型为 Content-Type:application/octet-stream。</span> 成功时HTTP返回状态码是201，具体操作的错误码以返回值为准。</div>
			  				<div class="row" style="margin-top:20px;">举例：参见 <a href="jsp/code/MobTest.jsp">Java实例</a></div>
																													
							<br /> 
							<br />
							<p id="data_operator_file_get" style="font-weight: bold;">读取文件</p>
							<div class="c_bg">
								<p><code>GET &nbsp; /file/&lt;tablename&gt;?hash_key=hashkey[&amp;range_key=rangekey]&amp;column=column_mob</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>&nbsp; &lt;实际的文件流&gt;//实际的文件流 </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top:20px;"><span style="color:#F00; border:medium;">特别注意：此处的头部的指定类型为 Content-Type:application/octet-stream。</span> 参数说明参见上传文件部分说明。</div>
			  				<div class="row" style="margin-top:20px;">举例：参见 <a href="jsp/code/MobTest.jsp">Java实例</a></div>
							-->
				    	</div>	    
				    		
						<br />
						<h3 id="index_operator">索引操作</h3>
						<!-- <a href="jsp/code/testSample.zip">索引操作示例代码</a> -->
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
			        		<div class="row" style="margin-top:20px;">索引操作的URL前缀：http://example.domain.com/otsrest/api/<B>index</B></div>
							<br />
							<div class="row" style="margin-top: 20px;">在OTS中，根据索引创建方式的不同，分为“基于Hbase索引”和“基于Solr索引”两种。两种类型的索引在创建、编辑、视图查询等方面略有不同，之后会区别描述。</div>
							<br />
							<p id="index_operator_getall" style="font-weight: bold;">获取所有索引</p>
							<div class="c_bg">
								<p><code>GET &nbsp; /&lt;tablename&gt;/_all_indexes </code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode": 0,&nbsp;&nbsp; //错误码 </code></p>
								<p><code>"index_names": &nbsp;&nbsp; //索引名列表 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight_index",&nbsp; &nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baokang_index" &nbsp; &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">返回表&lt;tablename&gt;的所有索引，包括索引的数目和索引名的列表。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/index/_all_indexes</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/index/baosight/_all_indexes?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 200 OK </code></P>
								<P><code>Cache-Control: no-cache </code></P>
								<P><code>Content-Type: application/json </code></P>
								<P><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode": 0,&nbsp;&nbsp; //错误码，0表示成功 </code></p>
								<p><code>"index_names":</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;[&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight_index",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"baosight_index_b"</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="index_operator_get" style="font-weight: bold;">获取索引信息</p>
							<div class="c_bg">
								<p><code>GET &nbsp; /&lt;tablename&gt;/&lt;indexname&gt; </code></p>
								<p></p>
								<p>（1）基于Solr索引</p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"type":0, &nbsp;&nbsp;//可选项，1表示基于Hbase索引，其他值（或缺省）表示基于Solr索引</code></p>
								<p><code>"index_name":"cvb",</code></p>
								<p><code>"shard_num":3, &nbsp;&nbsp;//分片数</code></p>
								<p><code>"replication_num":1, </code></p>
								<p><code>"pattern":0, &nbsp;&nbsp;//1表示在线模式，0表示离线模式 </code></p>
								<p><code>"create_time":"2015-03-03 11:08:46", &nbsp;&nbsp;//创建时间 </code></p>
								<p><code>"last_modify":"2015-03-03 11:08:46", &nbsp;&nbsp;//最后修改时间 </code></p>
								<p><code>"columns": [ &nbsp;&nbsp; //创建索引的列集合 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;//类型,包括int32/int64/float32/float64/string/boolean</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
								<p><code></code></p>
								<p><code>（2）基于Hbase索引</code></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"type":1, &nbsp;&nbsp;//必选项，1表示基于Hbase索引</code></p>
								<p><code>"index_name":"ind1",</code></p>
								<p><code>"shard_num":null,</code></p>
								<p><code>"replication_num":null, </code></p>
								<p><code>"columns": [ &nbsp;&nbsp; //创建索引的列集合 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;//列类型,包括int32/int64/float32/float64/string/binary</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxLen":4 &nbsp;&nbsp;//最大长度，根据列类型有所不同</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxLen":1 &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top:20px;">返回表&lt;tablename&gt;的索引&lt;indexname&gt;的信息，括索引的类型、列的集合（包括列的各自属性）。具体参数含义详见上方注释。
								<span style="color:#F00; border:medium;">索引&lt;indexname&gt;只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。</span>
							</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/index/baosight/baosight_index</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/index/baosight/baosight_index?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p>（1）基于Solr索引</p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"type":0, &nbsp;&nbsp;</code></p>
								<p><code>"index_name":"cvb",</code></p>
								<p><code>"shard_num":3,</code></p>
								<p><code>"replication_num":1, </code></p>
								<p><code>"pattern":0, &nbsp;&nbsp;</code></p>
								<p><code>"create_time":"2015-03-03 11:08:46", &nbsp;&nbsp;</code></p>
								<p><code>"last_modify":"2015-03-03 11:08:46", &nbsp;&nbsp;</code></p>
								<p><code>"columns": [ &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
								<p><code></code></p>
								<p>（2）基于Hbase索引</p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"type":1, &nbsp;&nbsp;</code></p>
								<p><code>"index_name":"ind1",</code></p>
								<p><code>"shard_num":null,</code></p>
								<p><code>"replication_num":null, </code></p>
								<p><code>"columns": [ &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxLen":4 &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxLen":1 &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="index_operator_create" style="font-weight: bold;">创建索引</p>
							<div class="c_bg">
								<p><code>POST &nbsp; /&lt;tablename&gt;/&lt;indexname&gt; </code></p>
								<p></p>
								<p>（1）基于Solr索引</p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"type":0,	&nbsp;&nbsp;//索引类型，0表示基于Solr索引，1表示基于Hbase索引</code></p>
								<p><code>"shard_num":3,	&nbsp;&nbsp;//分片数，建议与集群内Solr服务数量一致，创建后不可更改</code>	</p>
								<p><code>"replication_num":1, &nbsp;&nbsp;//可选项，缺省读取配置文件中该属性值 </code></p>
								<p><code>"pattern":0, &nbsp;&nbsp;//1表示在线模式，0表示离线模式 </code></p>
								<p><code>"columns": [ &nbsp;&nbsp; //创建索引的列集合 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;//类型,包括int32/int64/float32/float64/string/boolean</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp;&nbsp;&nbsp;//区分不同操作的结果</code></p>
								<p><code>}</code></p>
								<p><code></code></p>
								<p>（2）基于Hbase索引</p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"type":1, &nbsp;&nbsp;//1表示基于Hbase索引，0表示基于Solr索引 </code></p>
								<p><code>"columns": [ &nbsp;&nbsp; //创建索引的列集合 </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;//类型,包括int32/int64/float32/float64/string/binary</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"column":&nbsp;"column2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type":"string" &nbsp;&nbsp;</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"maxLen":32 &nbsp;&nbsp;//最大长度，当列类型为string/binary时必须指定，其他类型以系统默认值为准</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;]&nbsp;&nbsp;</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp;&nbsp;&nbsp;//区分不同操作的结果</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">
			   					<span style="color:#F00; border:medium;">注意：（1）创建索引的列集合中字段不能出现同名的列，否则会创建失败。</span>
			   				为表&lt;tablename&gt;创建索引名为&lt;indexname&gt;的索引，括索引的模式、列的集合（包括列的各自属性）。具体参数含义详见上方注释。
							</div>
							<div class="row">（2）创建“基于Solr索引”时，离线索引建议用于明确较小数据量的场景，否则可能会因服务端软硬件配置限制而导致离线索引创建失败。</div>
							<div class="row">（3）创建“基于Hbase索引”时，<span style="color: #F00; border: medium;">需要注意列集合中列的各自属性与原表数据保持一致，否则可能会导致索引编译失败。</span></div>
							<div class="row">（4）基于Hbase索引，<span style="color: #F00; border: medium;">一旦创建，不可编辑。</span></div>
							<div class="row">（5）创建“基于Hbase索引”时，<span style="color: #F00; border: medium;">当列类型为string/binary时，必须指定长度，最大长度目前未做限制，以应用场景为准，建议不要过大。</span></div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<p>（1）基于Solr索引</p>
								<P><code>% curl -u username@tenant:password -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index -d '{"type":0,"shard_num":3,"pattern":0,"columns":[{"column":"column1","type":"int32"},{"column":"column2","type":"string"}]}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"type":0,"shard_num":3,"pattern":0,"columns":[{"column":"column1","type":"int32"},{"column":"column2","type":"string"}]}'</code></p>
								<p>（2）基于Hbase索引</p>
								<P><code>% curl -u username@tenant:password -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index -d '{"type":1,"columns":[{"column":"column1","type":"int32"},{"column":"column2","type":"string","maxLen":32}]}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X POST -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"type":1,"columns":[{"column":"column1","type":"int32"},{"column":"column2","type":"string","maxLen":32}]}'</code></p>
								<P><code></code></P>
								<p>两种类型 索引返回值相同</p>
								<p><code>HTTP/1.1 201 CREATED </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
								<P><code></code></P>
							</div>

							<br /> 
							<br />
							<p id="index_operator_update" style="font-weight: bold;">更新索引</p>
							<div class="c_bg">
								<p><code>PUT &nbsp; /&lt;tablename&gt;/&lt;indexname&gt; </code></p>
								<p></p>
								<p>（1）基于Solr索引</p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"truncate":true,&nbsp;&nbsp;//true表示清空索引，false不做操作，可选项 </code></p>
								<p><code>"rebuild":true,&nbsp;&nbsp;//表示是否重建索引，索引配置保持原来的不变，可选项 </code></p>
								<p><code>"rebuildinfo":{ &nbsp;&nbsp;//修改索引信息，此时rebuild无效，可选项</code></p>
								<p><code>&nbsp; &nbsp; "replication_num":1, &nbsp;&nbsp;//可选项，缺省读取配置文件中该属性值 </code></p>
								<p><code>&nbsp; &nbsp; "pattern":0, &nbsp;&nbsp;//1表示在线模式，0表示离线模式 </code></p>
								<p><code>&nbsp; &nbsp; "columns": [ //创建索引的列集合</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"column":&nbsp;"column1",</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"type":"int32"&nbsp;&nbsp;&nbsp;&nbsp;//类型,包括int32/int64/float32/float64/string/boolean</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;},</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;{&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"column":&nbsp;"column2", </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;"type":"string" </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;}</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp;] &nbsp;</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp;}</code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp;&nbsp;&nbsp;//区分不同操作的结果</code></p>
								<p><code>}</code></p>
								<p><code></code></p>
								<p>（2）基于Hbase索引</p>
								<p><code>request</code></p>
								<p><code>{</code></p>
								<p><code>"truncate":true, &nbsp;&nbsp;//true表示清空索引，false不做操作，可选项 </code></p>
								<p><code>"rebuild":true &nbsp;&nbsp;//表示是否重建索引，索引配置保持原来的不变，可选项 </code></p>
								<p><code>}</code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp;&nbsp;&nbsp;//区分不同操作的结果</code></p>
								<p><code>}</code></p>
							</div>
						        <div class="row" style="margin-top:20px;">上述属性为属性全集，且均为可选项。
								<span style="color:#F00; border:medium;">一次请求有且只有一个可选项被执行，具体的优先关系为："rebuildinfo" &gt; "rebuild" &gt; "truncate" 。数据级较大时重建或更新索引，可能会因服务端软硬件配置限制而导致索引更新失败。</span>
								当不指定任何属性时仍然需要确保PUT的消息体是个合法的JSON语法串。更新成功时HTTP返回状态码是201，具体操作的错误码以返回值为准。</div>
							<div class="row"><span style="color:#F00; border:medium;">注意：基于Hbase索引创建后不可编辑，因此请求的消息体中没有“rebuildinfo”可选项。</span></div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index -d '{"rebuild":true}'</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -X PUT -i -H "Content-Type:application/json" http://example.domain.com/otsrest/api/index/baosight/baosight_index?token=7df7b07a56844a7ab44c48e2821b6759 -d '{"rebuild":true}'</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 201 CREATED </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
								<P><code></code></P>
							</div>

							<br /> 
							<br />
							<p id="index_operator_delete" style="font-weight: bold;">删除索引</p>
							<div class="c_bg">
								<p><code>DELETE &nbsp; /&lt;tablename&gt;/&lt;indexname&gt;</code></p>
								<P><code></code></P>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0 &nbsp;&nbsp;&nbsp;&nbsp;//区分不同操作的结果</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">删除索引成功时HTTP返回状态码是200。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -X DELETE -i http://example.domain.com/otsrest/api/index/baosight/baosight_index</code></p>
								<p><code>或者</code></p>
								<p><code> % curl -X DELETE -i http://example.domain.com/otsrest/api/index/baosight/baosight_index?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<P><code>HTTP/1.1 200 OK </code></p>
								<P><code>Cache-Control: no-cache </code></p>
								<P><code>Content-Type: application/json </code></p>
								<P><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0</code></p>
								<p><code>}</code></p>
								<P><code></code></P>
							</div>

							<br /> 
							<br />
							<p id="index_operator_queryrec" style="font-weight: bold;">基于索引查询记录</p>
							<div class="c_bg">
								<p>（1）基于Solr索引</p>
								<p><code>GET &nbsp; /&lt;tablename&gt;/&lt;indexname&gt;/?query=query_exp[&amp;filters=filter1,filter2,filter3][&amp;columns=column1,column2,column3][&amp;orders=column1:desc,column2:asc,column3:desc][&amp;limit=100&amp;offset=0]</code></p>
								<p></p>
								<p>（2）基于Hbase索引</p>
								<p><code>GET &nbsp; /&lt;tablename&gt;/&lt;indexname&gt;/?query=query_exp[&amp;hash_key=hashkey&amp;range_key_start=rangekey1&amp;range_key_end=rangekey2][&amp;columns=column1,column2,column3][&amp;limit=100&amp;offset=0][&amp;cursor_mark=cursor]</code></p>
								<p></p>
								<p><code>response（二者相同）</code>	</p>
								<p><code>{ </code></p>
								<p><code>&nbsp;"errcode":0, </code></p>
								<p><code>&nbsp;"match_count":3, &nbsp;//满足条件的总记录数</code></p>
								<p><code>&nbsp;"table_name":"baosight", &nbsp;//表名</code></p>
								<p><code>&nbsp;"records" : [</code></p>
								<p><code>&nbsp;&nbsp; {</code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "huang" : "abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "hash_key" : "10001",&nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp;&nbsp; "range_key" : "10001" &nbsp;&nbsp; </code></p>
								<p><code>&nbsp;&nbsp; }</code></p>
								<p><code>&nbsp;&nbsp;] </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">基于索引的记录查询需要指定表&lt;tablename&gt;和索引&lt;indexname&gt;，可以按需指定相应的查询配置项，具体含义如下：</div>
							<div class="row">（1）基于Solr索引</div>
						        <table style="width='780'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">
								<tr style="height: 30px;">
									<td width="260" bgcolor="#F6F6F6">字段</td>
									<td width="350" bgcolor="#F6F6F6">含义</td>
									<td width="170" bgcolor="#F6F6F6">&nbsp;&nbsp;备注</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">query=query_exp</td>
							    		<td bgcolor="#F6F6F6" style="word-break:break-all;">query_exp为solr查询的表达式。例如query=column1:Java AND column2:develop。此查询搜索指定的两个字段。如果查询所有，则输入为query=*:*</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;必有项，可以使用<span style="color: #1f497d; font-weight: bold;">运算符</span></td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">filters=filter1,filter2,filter3</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">设置查询结果的过滤条件，filter[n]的格式为"列名：过滤条件"，例如"column3:[2001 TO 2015]"，即指定column3满足2001至2015的查询</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项，可以使用<span style="color: #1f497d; font-weight: bold;">运算符</span></td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">columns=column1,column2,column3</td>
									<td bgcolor="#F6F6F6">指定返回记录时返回列的集合，默认返回全部列</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项，列的名称严格区分大小写</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">orders=column1:desc,column2:asc</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">返回记录按不同的列进行综合排序，格式为"列名：排序方式"，例如"column1:desc"</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">limit=100&amp;offset=0</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">限制返回，用于分页功能。默认limit为100，最大值10000，offset默认为0</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
							</table>
						        <div class="row" style="margin-top:20px;">query和filters字段使用的<span style="color:#1f497d;font-weight:bold;">运算符</span>包含如下：</div>
							<ul>
								<li>: 指定字段查指定值，如返回所有值*:*</li>
								<li>? 表示单个任意字符的通配</li>
								<li>* 表示多个任意字符的通配（不能在检索的项开始使用*或者?符号）</li>
								<li>~ 表示模糊检索，如检索拼写类似于"roam"的项这样写：roam~将找到形如foam和roams的单词；roam~0.8，检索返回相似度在0.8以上的记录。邻近检索，如检索相隔10个单词的"apache"和"jakarta"，"jakarta apache"~10</li>
								<li>^ 控制相关度检索，如检索jakarta	apache，同时希望去让"jakarta"的相关度更加好，那么在其后加上""符号和增量值，即jakarta4 apache</li>
								<li>布尔操作符AND、&amp;&amp;</li>
								<li>布尔操作符OR、||</li>
								<li>布尔操作符NOT、!、-（排除操作符，不能单独与项使用构成查询）</li>
								<li>+ 存在操作符，要求符号"+"后的项必须在文档相应的域中存在</li>
								<li>() 用于构成子查询</li>
								<li>[] 包含范围检索，如检索某时间段记录，包含头尾，date:[200707 TO 200710]</li>
								<li>{} 不包含范围检索，如检索某时间段记录，不包含头尾，date:{200707 TO 200710}</li>
								<li>\ 转义操作符，特殊字符包括+、-、&amp;&amp;、||、!、()、{}、[]、^、"、~、*、?、:、\</li>
							</ul>
							<div class="row" style="margin-top: 20px; color: #008200">
								<p>*************************************************************************************************************************************</p>
								<p>注：+ 和-表示对单个查询单元的修饰，AND、OR、NOT等是对两个查询单元是否做交集或者做差集还是取反的操作的符号。</p>
								<p>因此，语法格式总结为（实际使用时修饰符和字段名之间无空格，这里仅为查看清晰方便）：</p>
								<p style="font-weight: bold;">修饰符 字段名:查询关键词 AND/OR/NOT 修饰符 字段名:查询关键词</p>
								<p>如果你的检索式是:AB:china +AB:america，那么这个表示的是AB:china忽略不计可有可无，必须满足第二个条件才是对的，而不是你所认为的必须满足这两个检索表达式。</p>
								<p>真正表示两个表达式必须满足的是通过AND来表示的，如果输入:AB:china AND AB:america这个时候解析出来的是:+AB:china +AB:america</p>
								<p>要表达这样的关系你也可以用: +AB:china AND +Ab:america 或者 +AB:china +AB:america</p>
								<p>*************************************************************************************************************************************</p>
							</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i	http://example.domain.com/otsrest/api/index/baosight/baosight_index?"query=huang:abc*&amp;limit=10"</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/index/baosight/baosight_index?"query=huang:abc*&amp;limit=10&amp;token=7df7b07a56844a7ab44c48e2821b6759"</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<p><code>Content-Type: application/json </code></P>
								<p><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"match_count":4,</code></p>
								<p><code>"table_name":"baosight",</code></p>
								<p><code>"records" : [</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang" : "abc2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
							<div class="row">（2）基于Hbase索引</div>
						        <table style="width='780'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">
								<tr style="height: 30px;">
									<td width="260" bgcolor="#F6F6F6">字段</td>
									<td width="350" bgcolor="#F6F6F6">含义</td>
									<td width="170" bgcolor="#F6F6F6">&nbsp;&nbsp;备注</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">query=query_exp</td>
                                                                        <td bgcolor="#F6F6F6" style="word-break:break-all;">query_exp为查询的表达式。可以指定查询列的取值范围。
							            例如query=column1:[column1start TO column1end],column2:[column2start TO column2end]。此查询搜索指定的两个字段相应范围的结果。查询结果左闭右开。如果查询所有，则输入为query=*:*</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;必有项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6" style="word-break: break-all;">hash_key=hashkey&amp;range_key_start=rangekey1&amp;range_key_end=rangekey2</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">设置查询表达式的主键条件，三个字段均为可选项，使用规则可参照“读取记录”接口。</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">columns=column1,column2,column3</td>
									<td bgcolor="#F6F6F6">指定返回记录时返回列的集合，默认返回全部列。</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项，列的名称严格区分大小写</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">limit=100&amp;offset=0</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">限制返回，limit默认为100，offset默认为0</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">cursor_mark=cursor</td>
									<td bgcolor="#F6F6F6" style="word-break: break-all;">初次查询不需要设置，若一次查询无法返回符合查询条件的所有结果，则可以从查询结果中获取下次查询的游标，供下次查询时设置，下次查询根据cursor记录的位置继续查询后续记录。</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;可选项</td>
								</tr>
							</table>
							<div class="row" style="margin-top: 20px;">注：查询条件query_exp中，起始范围须避免出现字符串“ TO ”，否则会被视为关键字而造成错误查询结果。</div>

							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/index/baosight/baosight_index?</code></p>
								<p><code>"query=col1:[4 TO 8]&amp;hash_key=10001&amp;range_key_start=10003&amp;range_key_end=10008&amp;columns=col1,col2&amp;limit=3&amp;cursor_mark=00000001234"</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/index/baosight/baosight_index?</code></p>
								<p><code>"query=col1:[4 TO 8]&amp;hash_key=10001&amp;range_key_start=10003&amp;range_key_end=10008&amp;columns=col1,col2&amp;limit=3&amp;cursor_mark=00000001234&amp;token=7df7b07a56844a7ab44c48e2821b6759"</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></P>
								<p><code>Cache-Control: no-cache </code></P>
								<p><code>Content-Type: application/json </code></P>
								<p><code>Transfer-Encoding: chunked</code></P>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"match_count":4,</code></p>
								<p><code>"table_name":"baosight",</code></p>
								<p><code>"records" : [</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang" : "abc2",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10001" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;},</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;{</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"huang"&nbsp;:&nbsp;"abc1",</code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"hash_key" : "10001", </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;"range_key" : "10002" </code></p>
								<p><code>&nbsp;&nbsp;&nbsp;&nbsp;}</code></p>
								<p><code>&nbsp;&nbsp;]</code></p>
								<p><code>}</code></p>
							</div>
						</div>

						<br />
						<h3 id="monitor_operator">监控操作</h3>
						<!-- <a href="jsp/code/testSample.zip">监控示例代码</a> -->
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
							<div class="row" style="margin-top: 20px;">监控操作的URL前缀：http://example.domain.com/otsrest/api/<B>metrics</B></div>
							<br />
							<p id="monitor_operator_getall" style="font-weight: bold;">获取所有表的监控信息</p>
							<div class="c_bg">
								<p><code>GET &nbsp; / </code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code> "errcode":0, &nbsp; &nbsp; </code></p>
								<p><code> "metric_info_list": [ </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; {</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "table_name":"baosight",&nbsp;&nbsp;&nbsp;&nbsp;//表名 </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "record_count":133,&nbsp; &nbsp; //记录行数 </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "read_count":23322,&nbsp;&nbsp;&nbsp;&nbsp;//读计数</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "write_count": 1232,&nbsp; &nbsp;&nbsp; &nbsp; //写计数</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "disk_size": 5333 &nbsp; &nbsp;&nbsp; &nbsp; //数据大小（不含索引，字节）</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; },</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; {</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "table_name":"baokang",</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "record_count":103, </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "read_count":13322, </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "write_count":2232, </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "disk_size": 3333 </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; }</code></p>
								<p><code>&nbsp; &nbsp;] </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">返回（用户的）所有表的监控信息，目前仅包含读计数、写计数、数据大小（不含索引，单位：字节）。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/metrics</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/metrics?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code> "errcode":0, &nbsp; &nbsp; </code></p>
								<p><code> "metric_info_list": [ </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; {</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp;"table_name":"baosight",</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "record_count":133,</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "read_count":23322, </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "write_count"1232,</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "disk_size":5333</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; },</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; {</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp;"table_name":"baokang",</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "record_count":103,</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "read_count":13322, </code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "write_count":2232,</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; "disk_size":3333</code></p>
								<p><code>&nbsp; &nbsp; &nbsp; &nbsp; }</code></p>
								<p><code>&nbsp; &nbsp;]</code></p>
								<p><code>}</code></p>
							</div>

							<br /> 
							<br />
							<p id="monitor_operator_get" style="font-weight: bold;">获取单表的监控信息</p>
							<div class="c_bg">
								<p><code>GET &nbsp; /&lt;tablename&gt; </code></p>
								<p></p>
								<p><code>response</code></p>
								<p><code>{</code></p>
								<p><code>"errcode":0, &nbsp; &nbsp; </code></p>
								<p><code>"table_name":"baosight", &nbsp; &nbsp; //表名 </code>	</p>
								<p><code>"record_count":133, &nbsp; &nbsp; //记录行数 </code></p>
								<p><code>"read_count":23322, &nbsp; &nbsp; //读计数</code>	</p>
								<p><code>"write_count": 1232, &nbsp; &nbsp; //写计数 </code></p>
								<p><code>"disk_size": 5333 &nbsp; &nbsp; //数据大小（不含索引，字节） </code></p>
								<p><code>}</code></p>
							</div>
							<div class="row" style="margin-top: 20px;">返回表&lt;tablename&gt;的监控信息，目前仅包含读计数、写计数、数据大小（不含索引，单位：字节）。</div>
							<div class="row" style="margin-top: 20px;">举例：</div>
							<div class="c_bg">
								<P><code>% curl -u username@tenant:password -i http://example.domain.com/otsrest/api/metrics/baosight</code></P>
								<p><code>或者</code></p>
								<p><code> % curl -i http://example.domain.com/otsrest/api/metrics/baosight?token=7df7b07a56844a7ab44c48e2821b6759</code></p>
								<P><code></code></P>
								<p><code>HTTP/1.1 200 OK </code></p>
								<p><code>Cache-Control: no-cache </code></p>
								<p><code>Content-Type: application/json </code></p>
								<p><code>Transfer-Encoding: chunked</code></p>
								<P><code></code></P>
								<p><code>{</code></p>
								<p><code>"errcode":0, </code></p>
								<p><code>"table_name":"baosight",</code></p>
								<p><code>"record_count":133, </code></p>
								<p><code>"read_count":23322, </code></p>
								<p><code>"write_count"1232,</code></p>
								<p><code>"disk_size":5333</code></p>
								<p><code>}</code></p>
							</div>
						</div>
					</div>

					<div class="bs-docs-section">
						<h1 id="java_example" class="page-header">JAVA示例</h1>
						<div class="row" style="margin-top:20px;">
			       	  		<div style="width:80%;border:1px solid #EDEDED;padding:10px;background:#F7F7F7;color:#4F4F4F;">
								<span style="color:#104E8B;font-size:14px;font-weight:bold" >【示例代码】</span><br>
								<span style="font-size:12px">
									<pre class="brush: java;"> 
package Httptest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import sun.misc.BASE64Encoder;

public class OtsrestTest {

	public static final int HTTP_CONNECT_TIMEOUT = 30000;// ms
	public static final int HTTP_READ_TIMEOUT = 30000;// ms

	/**
	 * if success, return data will be a string like
	 * {"token":"c283bdc68a284c7bbe9257fee8a9f5d2","errcode":0}
	 */
	public String get_token(String serverIP, String username, String password) 
	{

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/token");
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO !! must using basic authorization to get token.
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) 
			{
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like
	 * {"errcode":0,"total_count":2,"table_names":["ABC","BCD"]}
	 */
	public String get_tablelist(String serverIP, String username,
			String password) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/_all_tables");
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) 
			{
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like
	 * {"errcode":0,"total_count":2,"table_names":["ABC","BCD"]}
	 */
	public String get_tablelist(String serverIP, String token) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/_all_tables");
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) 
			{
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like { "errcode":0,
	 * "description":"test", "table_name":"test123456", "compression_type":0,
	 * "primary_key_type":1, "hash_key_type":1, "range_key_type":1,
	 * "create_time":"2016-10-09 15:53:00", 
	 * "modify_time":"2016-10-09 15:53:00"}
	 * 
	 */
	public String get_table(String serverIP, String username, String password,
			String tablename) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) 
			{
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like 
	 * { "errcode":0,
	 * "description":"test", 
	 * "table_name":"test123456", 
	 * "compression_type":0,
	 * "primary_key_type":1, 
	 * "hash_key_type":1, 
	 * "range_key_type":1,
	 * "create_time":"2016-10-09 15:53:00"}
	 * 
	 */
	public String get_table(String serverIP, String token, String tablename) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) 
			{
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_table(String serverIP, String username,
			String password, String tablename, String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_table(String serverIP, String token, String tablename,
			String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_table(String serverIP, String username,
			String password, String tablename) {
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_table(String serverIP, String token, String tablename)
	{
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/table/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
					returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_record(String serverIP, String username,
			String password, String tablename, String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/record/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_record(String serverIP, String token,
			String tablename, String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP)
					.append("/otsrest/api/record/")
					.append(tablename);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be
	 * {"errcode":0,"records":[{"column1":"abc1",
	 * "hash_key":"10001","column2":"abc2","range_key":"10001"}]}
	 */
	public String get_record(String serverIP, String username, String password,
			String tablename, String condition) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/record/")
					.append(tablename).append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be
	 * {"errcode":0,"records":[{"column1":"abc1",
	 * "hash_key":"10001","column2":"abc2","range_key":"10001"}]}
	 */
	public String get_record(String serverIP, String token, String tablename,
			String condition) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/record/")
					.append(tablename).append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_record(String serverIP, String username,
			String password, String tablename, String condition) {
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/record/")
					.append(tablename).append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_record(String serverIP, String token,
			String tablename, String condition) {
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/record/")
					.append(tablename).append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like
	 * {"errcode":0,"index_names":["index_test"]}
	 */
	public String get_indexlist(String serverIP, String username,
			String password, String tablename) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/_all_indexes");
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like
	 * {"errcode":0,"index_names":["index_test"]}
	 */
	public String get_indexlist(String serverIP, String token, 
			String tablename) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/_all_indexes");
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like 
	 * { "errcode":0, 
	 * "type":0,
	 * "index_name":"index_test", 
	 * "shard_num":3, 
	 * "replication_num":1,
	 * "columns":[ 
	 * {"type":"string","column":"column1"},
	 * {"type":"string","column":"column2"} 
	 * ], 
	 * "pattern":1,
	 * "create_time":"2016-10-09 17:07:45", 
	 * "last_modify":"2016-10-09 17:07:45"
	 * }
	 * 
	 */
	public String get_index(String serverIP, String username, String password,
			String tablename, Object indexname) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be a string like 
	 * { "errcode":0, 
	 * "type":0,
	 * "index_name":"index_test", 
	 * "shard_num":3, 
	 * "replication_num":1,
	 * "columns":[ 
	 * {"type":"string","column":"column1"},
	 * {"type":"string","column":"column2"} 
	 * ], 
	 * "pattern":1,
	 * "create_time":"2016-10-09 17:07:45", 
	 * "last_modify":"2016-10-09 17:07:45"
	 * }
	 * 
	 */
	public String get_index(String serverIP, String token, String tablename,
			String indexname) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_index(String serverIP, String username,
			String password, String tablename, String indexname, String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String create_index(String serverIP, String token, String tablename,
			String indexname, String data) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "POST" operate
			http_conn.setRequestMethod("POST");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			OutputStream dos = http_conn.getOutputStream();
			dos.write(data.toString().getBytes());
			dos.flush();
			dos.close();

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_index(String serverIP, String username,
			String password, String tablename, String indexname) {
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be {"errcode":0}
	 */
	public String delete_index(String serverIP, String token, String tablename,
			String indexname) {
		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "DELETE" operate
			http_conn.setRequestMethod("DELETE");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be
	 * {"errcode":0,"match_count":1,"records":[{"column1"
	 * :"abc1","hash_key":"10001","column2":"abc2","range_key":"10001"}]}
	 */
	public String get_record_byindex(String serverIP, String username,
			String password, String tablename, String indexname,
			String condition) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname)
					.append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using basic authorization
			String authorization = new BASE64Encoder()
					.encode((username + ":" + password).getBytes());
			http_conn.setRequestProperty("Authorization", "Basic "
					+ authorization);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * if success, return data will be
	 * {"errcode":0,"match_count":1,"records":[{"column1"
	 * :"abc1","hash_key":"10001","column2":"abc2","range_key":"10001"}]}
	 */
	public String get_record_byindex(String serverIP, String token,
			String tablename, String indexname, String condition) {

		try {
			StringBuffer urlBuffer = new StringBuffer().append("http://")
					.append(serverIP).append("/otsrest/api/index/")
					.append(tablename).append("/").append(indexname)
					.append("?").append(condition);
			URL urlObj = new URL(urlBuffer.toString());
			HttpURLConnection http_conn = (HttpURLConnection) urlObj
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setDoInput(true);
			http_conn.setUseCaches(false);

			// TODO here must be "GET" operate
			http_conn.setRequestMethod("GET");
			// TODO here must be "application/json" type
			http_conn.setRequestProperty("Content-Type", 
					"application/json");

			// TODO here using token
			http_conn.setRequestProperty("token", token);

			http_conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
			http_conn.setReadTimeout(HTTP_READ_TIMEOUT);

			String returnData = "";
			int responseCode = http_conn.getResponseCode();
			System.out.println("*****" + responseCode);
			if (HttpURLConnection.HTTP_OK == responseCode
				|| HttpURLConnection.HTTP_CREATED == responseCode) {
				InputStream is = http_conn.getInputStream();
				byte[] buffer = new byte[1024];
				int len = 0;
				while ((len = is.read(buffer)) != -1) {
				  returnData += new String(buffer, 0, len, "UTF-8");
				}
				is.close();

				// System.out.println(returnData);
			}

			if (returnData.isEmpty()) {
				return null;
			}

			return returnData;

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * !! you may pack or parse json string by a tool, jackson-all-1.9.11.jar
	 * for example.
	 * 
	 */
	public static void main(String[] args) throws Exception {
		String serverIP = "127.0.0.1"; // for test
		String username = "admin@test"; // for test
		String password = "admin"; // for test
		String test_tablename = "test_table";
		String test_indexname = "test_index";
		OtsrestTest ots = new OtsrestTest();

		String result = ots.get_token(serverIP, username, password);
		System.out.println(result);
		// TODO parse result to get token, token will expire if not used for a
		// while
		// String token = "a6a80ac34c2c4863875f7533efe721f2";

		// see the user manual for the field meaning used
		String create_table_data = "{\"primary_key_type\":1, "
				+ "\"hash_key_type\":0, "
				+ "\"range_key_type\":0, "
				+ "\"description\":\"for test\", "
				+ "\"compression_type\":0}";
		result = ots.create_table(serverIP, username, password, test_tablename,
				create_table_data);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.create_table(serverIP, token, test_tablename,
		// create_table_data);
		// System.out.println(result);

		result = ots.get_tablelist(serverIP, username, password);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_tablelist(serverIP, token);
		// System.out.println(result);

		result = ots.get_table(serverIP, username, password, test_tablename);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_table(serverIP, token, test_tablename);
		// System.out.println(result);

		String create_record_data = "{\"records\":["
				+ "{\"hash_key\":\"10001\","
				+ "\"range_key\":\"10001\","
				+ "\"column1\":\"abc1\","
				+ "\"column2\":\"abc2\"}"
				+ "]}";
		result = ots.create_record(serverIP, username, password,
				test_tablename, create_record_data);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.create_record(serverIP, token, test_tablename,
		// create_record_data);
		// System.out.println(result);

		// TODO here only a simple query example, complex condition may need
		// urlencode
		String query_record_condition = "limit=100&offset=0&hash_key=10001";
		result = ots.get_record(serverIP, username, password, test_tablename,
				query_record_condition);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_record(serverIP, token, test_tablename,
		// query_record_condition);
		// System.out.println(result);

		// TODO here only a simple delete example, complex condition may need
		// urlencode
		String delete_record_condition = "hash_key=10001";
		result = ots.delete_record(serverIP, username, password,
				test_tablename, delete_record_condition);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.delete_record(serverIP, token, test_tablename,
		// delete_record_condition);
		// System.out.println(result);

		// see the user manual for the field meaning used
		String create_index_data = "{\"type\":0, "
				+ "\"shard_num\":3, "
				+ "\"pattern\":1, "
				+ "\"columns\":["
				+ "{\"column\":\"column1\", \"type\":\"string\"},"
				+ "{\"column\":\"column2\", \"type\":\"string\"}"
				+ "]}";
		result = ots.create_index(serverIP, username, password, test_tablename,
				test_indexname, create_index_data);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.create_index(serverIP, token, test_tablename,
		// test_indexname, create_index_data);
		// System.out.println(result);

		result = ots
				.get_indexlist(serverIP, username, password, test_tablename);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_indexlist(serverIP, token, test_tablename);
		// System.out.println(result);

		result = ots.get_index(serverIP, username, password, test_tablename,
				test_indexname);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_index(serverIP, token, test_tablename,
		// test_indexname);
		// System.out.println(result);

		// for query by index
		// result = ots.create_record(serverIP, username, password,
		// test_tablename, create_record_data);
		// System.out.println(result);
		// TODO use token parsed above
		// result = ots.create_record(serverIP, token, test_tablename,
		// create_record_data);
		// System.out.println(result);

		// TODO here only a simple query example, complex condition may need
		// urlencode
		String index_query_record_condition = "query=*:*&limit=100&offset=0";
		result = ots.get_record_byindex(serverIP, username, password,
				test_tablename, test_indexname, index_query_record_condition);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.get_record_byindex(serverIP, token, test_tablename,
		// test_indexname, index_query_record_condition);
		// System.out.println(result);

		result = ots.delete_index(serverIP, username, password, test_tablename,
				test_indexname);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.delete_index(serverIP, token, test_tablename,
		// test_indexname);
		// System.out.println(result);

		result = ots.delete_table(serverIP, username, password, test_tablename);
		System.out.println(result);
		// TODO use token parsed above
		// result = ots.delete_table(serverIP, token, test_tablename);
		// System.out.println(result);

		System.exit(0);
	}
}					
									</pre>
								</span>	
							</div>
		       	  			</div>	
		   			</div>
					
					<div class="bs-docs-section">
						<h1 id="error_code" class="page-header">错误码</h1>
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
							<p id="ots_error_code" style="font-weight: bold;">OTS REST操作</p>
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td width="300" bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173001</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的索引操作</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173002</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新记录失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173003</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;获取表的监控信息失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173004</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;检查租户（命名空间）的监控信息失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173005</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;监控加锁失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173006</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的记录查询类型</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173007</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的记录删除类型</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173008</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;传入无效的记录，请检查hash_key及range_key</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173009</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;获取索引列表失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173010</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;获取token认证失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173021</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;正在运行恢复/备份任务</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173022</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;连接FTP服务失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173023</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;所需FTP文件夹不存在</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173024</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;备份/恢复执行MR过程出错</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">173025</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;备份任务错误</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">173026</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;恢复任务错误</td>
								</tr>
							</table>
							<br />
							<p id="table_error_code" style="font-weight: bold;">表操作（Hbase）</p>
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td width="300" bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174001</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建表失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174002</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除表失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174003</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新表失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174004</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;表已经存在</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174005</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;表不存在</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174006</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;保存表的配置失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174007</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;插入记录失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174008</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除记录失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174009</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询记录失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174010</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;获取文件失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174011</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;上传文件失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174012</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的按范围查询参数</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174013</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的精确查询参数</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174014</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的按范围删除参数</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174015</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的精确删除参数</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174016</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的正则表达式参数</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174017</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除记录失败，没有符合的记录</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174018</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引列名重复</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174019</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引重建失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174020</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引不存在</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174021</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;master没有运行</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174022</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;当前无服务的Region，可重试操作</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174023</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;当前Region忙，可重试操作</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174024</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;连接不上zookeeper</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174025</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的租户信息（命名空间）</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174026</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;Json格式转换为对象失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174027</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的zookeeper初始参数</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">174028</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的postgresql初始参数</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">174029</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的Solr服务器地址</td>
									<td bgcolor="#F6F6F6">174030</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询表enable状态失败</td>
								</tr>
							</table>
							<br />
							<p id="index_error_code" style="font-weight: bold;">基于Solr索引操作</p>
							<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
								<tr style="height: 30px;">
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td width="300" bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
									<td width="80" bgcolor="#F6F6F6">Code</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175001</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的列名</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175002</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的列类型，列类型只能为：int32/int64/float32/float64/string/boolean</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175003</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的列数，索引至少须有一列</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175004</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;生成molphine mapper文件失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175005</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;生成molphine文件失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175006</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引配置模板不存在</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175007</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;复制配置模板文件失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175008</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;Schema.xml不存在</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175009</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;解析Schema.xml失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175010</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;更改Schema.xml失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175011</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;从zookeeper删除配置失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175012</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;连接Solr服务器失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175013</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的Solr服务器地址</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175014</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建Solr连接失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175015</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除Solr连接失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175016</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;运行hbase indexer命令失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175017</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引已经存在</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175018</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引正在建立</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175019</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引建立失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175020</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引清空失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175021</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除索引配置失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175022</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除索引失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175023</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;上传索引配置至zookeeper失败</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175024</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建索引失败</td>
								</tr>
								<tr style="height: 30px;">
									<td bgcolor="#F6F6F6">175025</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;无法连接zookeeper</td>
									<!-- </tr>
								<tr style="height:30px;"> -->
									<td bgcolor="#F6F6F6">175026</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新索引失败</td>
								</tr>
								<tr style="height:30px;">
									<td bgcolor="#F6F6F6">175027</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;基于索引查询失败</td>
									<td bgcolor="#F6F6F6">175028</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;索引不存在</td>
								</tr>	
								<tr style="height:30px;">
									<td bgcolor="#F6F6F6">175029</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的share值大小</td>
									<td bgcolor="#F6F6F6">175030</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的replication值大小</td>
								</tr>
								<tr style="height:30px;">
									<td bgcolor="#F6F6F6">175031</td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的pattern值大小</td>
									<td bgcolor="#F6F6F6"></td>
									<td bgcolor="#F6F6F6">&nbsp;&nbsp;</td>
								</tr>
							</table>
							<br/>
					<p id="index_hbase_error_code" style="font-weight:bold;">基于Hbase索引操作</p>
					<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
						<tr style="height:30px;">
							<td width="80" bgcolor="#F6F6F6">Code</td>
							<td width="300" bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
							<td width="80" bgcolor="#F6F6F6">Code</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175501</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;hbase索引己存在</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175502</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建hbase索引表失败</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175503</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;缺少hbase索引列</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175504</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;无法识别的hbase索引类型</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175505</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;hbase索引正在编绎</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175506</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;hbase索引表Disable</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175507</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;hbase索引列己存在</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175508</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;添加hbase索引列失败</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175509</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法hbase索引名</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175510</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法MAX_LEN</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175511</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法hbase索引信息</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175512</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;获取hbase索引信息失败</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175513</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的类型长度 </td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175514</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除hbase索引失败</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175515</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除hbase索引表失败</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175516</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新hbase索引表失败</td>
						</tr>
						<tr style="height:30px;">
							<td bgcolor="#F6F6F6">175517</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;hbase列范围非法</td>
						<!-- </tr>
						<tr style="height:30px;"> -->
							<td bgcolor="#F6F6F6">175518</td>
							<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法值</td>
						</tr>
					</table>
							<!-- <br/>
						<p id="configure_error_code" style="font-weight:bold;">配置操作</p>
						<table style="width='650'; border='0'; cellpadding='0'; cellspacing='1'; bgcolor='#BDBEC0';">					  
							<tr style="height:30px;">
								<td width="80" bgcolor="#F6F6F6">Code</td>
								<td width="300" bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
								<td width="80" bgcolor="#F6F6F6">Code</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;含义</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176501</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;非法的postgresql quorum格式，quorum为0</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176502</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;加载JDBC驱动失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176503</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;关闭数据库失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176504</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;连接数据库失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176505</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建表失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176506</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;SQL操作失败，无法回滚</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176507</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除表失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176508</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新表失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176509</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询表失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176510</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;创建索引失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176511</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除索引失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176512</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询索引失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176513</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新索引失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176514</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;添加表配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176515</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除表配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176516</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询表配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176517</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新表配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176518</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;添加索引配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176519</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;删除索引配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176520</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询索引配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176521</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;更新索引配置失败</td>
							</tr>
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176522</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询租户列表失败</td>
							</tr>		
							<tr style="height:30px;">
								<td bgcolor="#F6F6F6">176523</td>
								<td bgcolor="#F6F6F6">&nbsp;&nbsp;查询租户ID失败</td>
							</tr>					
						</table> -->
						</div>
					</div>
					<div class="bs-docs-section">
						<h1 id="appendix" class="page-header">附录</h1>
						<div class="c_bg">
							<h3 id="appendix_namerule">&nbsp;&nbsp;命名规则</h3>
							<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
								<div class="row">1）不能为空。</div>
								<div class="row">2）只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。</div>
								<div class="row">3）长度不能超过128个字符。</div>
							</div>
						</div>

					</div>
					<div class="bs-docs-section">
						<h1 id="faq" class="page-header">FAQ</h1>
						<h3 id="deploy">Q: 目前使用索引有什么限制？</h3>
						<div style="margin-top: 20px; margin-left: 20px; margin-right: 20px; margin-bottom: 20px;">
							<div class="row">索引Solr类型创建时的column类型暂不支持binary类型，后续将技术升级后支持。</div>
							<div class="row">索引Hbase类型创建时（1）需要注意列集合中列的各自属性与原表数据保持一致，否则可能会导致索引编译失败。（2）基于Hbase索引，一旦创建，不可编辑。（3）当列类型为string/binary时，必须指定长度，最大长度目前未做限制，以应用场景为准，建议不要过大。</div>
							<div class="row">索引Hbase类型创建时，列名不可包含#、->、=>、,等符号。</div>
						</div>
					</div>

				</div>
			</div>
		</div>
		<div style="height: 50px;"></div>
	</div>
</body>

</html>