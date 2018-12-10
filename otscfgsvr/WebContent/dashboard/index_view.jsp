<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    	<meta charset="UTF-8"/>
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">        
		<title>索引视图--OTS 配置中心</title>		
		<link rel="shortcut icon" href="jsp/images/icon.ico"/>
		<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
		<link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
		<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
		
		<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
		<script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
		<script type="text/javascript" src="jsp/js/bootstrap-table.js"></script>
		<script type="text/javascript" src="jsp/js/init-js.js"></script>
		<script type="text/javascript">
			$(function(){
				showActivePage("table");
			});
		</script>
</head>

<body class="body_fix">
	<div id="header">
		<jsp:include page="header.jsp" />
	</div>
		
	<!---content--->   
	<div class="content">
		<div class="row">
			<div class="row" style="height:40px;">  
				<div class="pageicon">|</div>
				<div style="margin-left:5px;margin-top:5px;">
					<ul class="breadcrumb pagetitle" style="background-color:transparent;padding-top:0px;padding-left:5px;">
						<li><a href="table.jsp" >OTS-表的管理</a></li>
						<li><a href="#" id="table_name"></a></li>
						<li><a href="#" id="view_name"></a></li>
					</ul>
		 		</div>	
			</div>
			<div class="row" style="margin-left:70px;">
			<table>
				<tr>
					<td class="col-md-1" style="">查询条件</td>
					<td class="col-md-5">
						<textarea name="" id="indexQueryContent" cols="" rows="" style="width:100%;resize:none;"></textarea>
					</td>
					<td class="col-md-1" style="">显示列&nbsp;<a href="#"><span class="glyphicon glyphicon-edit" style="font-size:10px;" title="编辑显示列" onclick='editIndexViewColumns();' data-toggle="modal"  data-target="#indexViewColumnEdit"></span></a></td>
					<td class="col-md-5">
						<input name="" id="indexQueryColumns1" type="button" class="btn" style="width:19%;cursor:default;" value="">
      					<input name="" id="indexQueryColumns2" type="button" class="btn" style="width:19%;cursor:default;" value="">
      					<input name="" id="indexQueryColumns3" type="button" class="btn" style="width:19%;cursor:default;" value="">
      					<input name="" id="indexQueryColumns4" type="button" class="btn" style="width:19%;cursor:default;" value="">
      					<input name="" id="indexQueryColumns5" type="button" class="btn" style="width:19%;cursor:default;" value="">
      				</td>
				</tr>
				<tr id="trFilterSortForSolr">
					<td class="col-md-1" style="">过滤条件</td>
					<td class="col-md-5">
      					<textarea name="" id="indexViewFilters" cols="" rows="" style="width:100%;resize:none;" placeholder="详见“文档”-REST接口“基于索引查询记录”（filters）"></textarea>
					</td>
					<td class="col-md-1" style="">排序条件</td>
					<td class="col-md-5">
						<textarea name="" id="indexViewSort" style="width:100%;resize:none;" placeholder="详见“文档”-REST接口“基于索引查询记录”（orders）"></textarea>
					</td>
      			</tr>
				<tr id="trHashRangeForHbase">
					<td class="col-md-1" style="">Hash键</td>
					<td class="col-md-5">
						<input name="" id="indexViewHashKey" type="text" class="" style="width:100%;margin-bottom:10px;" placeholder="Hash键">
					</td>
					<td class="col-md-1" id="indexViewRangeKeyText" style="">Range键</td>
					<td class="col-md-5" id="indexViewRangeKeys">
						<input name="" id="indexViewRangeKeyStart" type="text" class="" style="width:48%;" placeholder="起始键">&nbsp;&nbsp;
						<input name="" id="indexViewRangeKeyEnd" type="text" class="" style="width:48%;" placeholder="终止键">
					</td>				
				</tr>
				<tr>
					<td></td>
					<td class="col-md-5" style="">
						<div class="r_text" style="width:60px;margin-left:0px;">限制返回</div>
						<div class="r_box" style="width:60px;"><input name="" id="indexViewLimit" type="text" class="textbox1" style="width:60px;text-align:right" placeholder="100"></div>
     					<div class="r_text" style="margin-left:0px;">条记录</div>
     					<div class="r_text" style="width:40px;">跳过</div>
     					<div class="r_box" style="width:60px;"><input name="" id="indexViewSkip" type="text" class="textbox1" style="width:60px;text-align:right" placeholder="0"></div>
     					<div class="r_text" style="margin-left:0px;">条记录</div>
     				<td style="">
     					<div><input id="btn_queryindexrec" name="" type="button" class="btn" style="width:60px;" value="查询" onclick="indexViewQuery();"></div>
     				</td>
     				<td></td>
     			</tr>
			</table>
			</div>
    		<div class="row" style="margin-left:140px;margin-top:0px;height:15px;">
    			<div class="r_text14" id="limitUnvalied" style="margin-left:60px;display:none;">请输入0~10000整数值。</div>
    			<div class="r_text14" id="skipUnvalied" style="margin-left:240px;display:none;">请输入非负整数值。</div>
    			<div class="r_text14" id="skipTooLarge" style="margin-left:240px;display:none;">输入的整数值过大。</div>
    			<div id="indexViewHbaseTips" style="margin-right:70px;text-align:right;color:#428BCA;display:none;">注：仅当指定Hash键时，Range键查询条件才有效！</div>
    		</div>
     		<div class="row" style="margin-left:70px;margin-right:70px;"> 
      		<!-- 
      			<div class="table-controls">
      				<div class="pull-right">
      					<input name="" type="button" class="btn" style="width:80px;" value="删除记录" onclick='recordViewDelete();'>
      				</div>
      			</div>
      		 -->
				<table id="indexView" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
					<thead>
						<tr>
							<th data-field="hash_key" data-align="center" data-sortable="true">Hash键</th>
							<th data-field="" data-align="center"></th>
							<th data-field="" data-align="center"></th>
							<th data-field="" data-align="center"></th>
							<th data-field="" data-align="center"></th>
							<th data-field="" data-align="center"></th>
							<th data-field="" data-align="center"></th>
							<th data-class="col-operate-view" data-field="operate" data-align="center" data-formatter="viewOperateFormatter" data-events="operateEvents">操作</th>
						</tr>
					</thead>
				</table>
 			</div>  
  		</div>
	</div>
	<div>
		<jsp:include page="footer.jsp" />
	</div>
	<div> 
		<jsp:include page="errorTips.jsp" />
	</div>

	<div class="modal text-center" id="indexViewModal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
   		<div class="modal-dialog" style="display: inline-block; width: auto;">
      		<div class="modal-content">
      			<div class="modal-header" style="background-color:#f5f5f5;">
            		<button type="button" class="close" data-dismiss="modal" aria-hidden="true"> &times;</button>
            		<h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">记录详细信息</h4>  
            	</div>
        		<div class="modal-body">
    				<div class="row" style="height:200px;margin-top:0px;"><textarea name="" id="indexViewContent" cols="" rows="" style="width:550px;height:200px;max-width:550px;max-height:200px;resize:none;"></textarea></div>
    				<div class="row" style="height:30px;"></div>
        		</div>
      		</div>
		</div>
	</div>
	<!---dialog: property--->
<div class="modal text-center" id="indexViewColumnEdit" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
   <div class="modal-dialog" style="display: inline-block; width: auto;">
      <div class="modal-content"></div>
	</div>
</div>
</body>

<script type="text/javascript">
	var index_view_query = "";
	var index_view_skip = 0;
	var index_view_limit = 100;
	var index_view_total_record_num = 0;
 	var index_view_columns_num = 0;
	var index_view_tablename = getUrlParam("tablename");
	var index_view_indexname = getUrlParam("indexname");
	var indexViewListHeadHtml = "";
	var index_view_records = [];
	var recordToShow = {};
	var recordShownKeys = [];
	var index_view_modal_table_name = {};
	var index_view_modal_index_name = {};
	var index_view_pktype = getUrlParam("pktype");
	var index_view_range = getUrlParam("range");
	var index_view_index_type = getUrlParam("idxtype");		//0-solr, 1-hbase
	var index_view_show_columns_num = 7;
	var index_view_show_columns = [];
	var index_view_query_columns = [];
	var index_view_column_property = {"field":"", "title":"", "align":"center"};
	
	function indexViewAlertMsg(msg){
		errorAlertMsg(msg);
	}
	
	function indexViewhelpInfo(message)
	{
		$("#indexViewContent").attr("value", message);
    	$("#indexViewModal").modal("show");
	}
	
	$(function(){
		if (index_view_tablename == null || index_view_indexname == null ||index_view_pktype == null || index_view_index_type == null) {
   			recordAlertMsg("非法的URL，请跳转至“表详细页”页面，点击索引名，进入“索引视图”页面。");
   		 	//window.location = "table_detail.jsp";
   		}
		
    	var href = "table_detail.jsp?tablename=" + index_view_tablename   + "&pktype=" + index_view_pktype  + "&display=index"+ "&id=" + getUrlParam("id");
    	if (index_view_pktype == 1) {
    		href += "&range=" + index_view_range;
    	}
		$("#table_name").attr("href", href);
       	$("#table_name").text(getStructName(index_view_tablename) + "-索引");
		document.getElementById("table_name").title = index_view_tablename;
       	$("#view_name").text(getStructName(index_view_indexname) + "-视图");
		document.getElementById("view_name").title = index_view_indexname;
		
	    for (var i=0; i<index_view_show_columns_num; i++) {
	    	index_view_show_columns[i] = index_view_column_property;
   		}
	    index_view_show_columns[0] = {"field":"hash_key", "title":"Hash键", "align":"center", "sortable":true};	
	    if (index_view_pktype == 1) {
	    	index_view_show_columns[1] = {"field":"range_key", "title":"Range键", "align":"center", "sortable":true};
	    }
		if (index_view_index_type == 1) {
			$("#indexQueryContent").attr("placeholder",  "缺省默认查询所有记录。详见“文档”-REST接口“基于索引查询记录”（基于Hbase索引-query）");
			document.getElementById("trFilterSortForSolr").style.display = "none";
			document.getElementById("indexViewHbaseTips").style.display = "block";
			if (index_view_pktype == 0) {
				$("#indexViewRangeKeyStart").attr("disabled", "disabled");
				$("#indexViewRangeKeyEnd").attr("disabled", "disabled");
				//document.getElementById("indexViewRangeKeyText").style.display = "none";
				//document.getElementById("indexViewRangeKeys").style.display = "none";
			}
		}
		else {
			$("#indexQueryContent").attr("placeholder",  "缺省默认查询所有记录。详见“文档”-REST接口“基于索引查询记录”（基于Solr索引-query）");
			document.getElementById("trHashRangeForHbase").style.display = "none";
		}

			$.ajax({
	 			cache: false,
	 			type: "GET",
	 			url: "/otscfgsvr/api/index/" + index_view_tablename + "/" + index_view_indexname + "/display_columns",
	 			dataType: "json",
	 			timeout:30000,
	            success:function(results, msg){ 
	            	if (results["errcode"] == 0) {
	            		var property = results["columns"];
	                	if (property.length > 0) {
	                		index_view_query_columns = property.split(",");
	                	}
	                	getIndexViewShowColumns();
	                	/* else {
	                		indexViewInit();
	                	} */
	            	}
	            	else {
	            		indexViewAlertMsg("获取索引显示列信息失败！错误: " + errorInfo(results["errcode"]) + "错误码: " + results["errcode"]);
	            	}
	            },
		         complete: function()
			     {
		            indexViewColumnsInit();
		            $('#indexView').bootstrapTable({
		                data: [], 
		                classes: 'table',
		                striped: true,
		                formatNoMatches: function () {
		                	return '请输入查询条件。';
		                },
		                undefinedText: '',
		                columns: index_view_show_columns
		            });
			     },
	            error: function(msg){
	            	if(msg["status"] != 404) {
	                	var errmsg = "获取索引显示列信息失败！错误: " + getStructMsg(msg);
	                	indexViewAlertMsg(errmsg);
	            	}
	            	/* else {
	            		indexViewInit();
	            	} */
	      		}
	  		});
		
		
		
		$('#indexView').on('page-change.bs.table', function (e, size, number){
 			var $table = $('#indexView').bootstrapTable();
 			var totalPages = $table.bootstrapTable('getOptions').totalPages;
 			if(totalPages == size){
 				index_view_total_record_num = $table.bootstrapTable('getOptions').totalRows;
 				indexViewUpdate();
 			}
 		});
	});
	
	function viewOperateFormatter(value, row, index) {
        return '<input name="" type="button" class="viewRecord btn3" style="width:50px;" value="查看" data-toggle="modal" data-target="#indexViewModal">';
    }
	
	window.operateEvents = {
            'click .viewRecord': function (e, value, row, index) {
            	goRecordDetail(row);
            }
        };
	/* function indexViewInit() {
		$.ajax({
			async: false,
            type: "GET",
            url: "/otscfgsvr/api/index/" + index_view_tablename + "/" + index_view_indexname + "?query_from=0",
            dataType: "json",
            timeout: 30000,
            success: function (results, msg) {
             	$.each(results.columns, function (InfoIndex, Info) {
             		index_view_columns.push(Info["column"]);
             	});
             	index_view_columns = index_view_columns.slice(0,5);
             },
        	 error: function (msg) {
        		 for (var i=0; i<5; i++){
 	            	indexViewListHeadHtml += "<th data-field='' data-align='center'></th>"; 
              	}
             	var errmsg = "获取索引信息错误: " + getStructMsg(msg);
               	indexViewAlertMsg(errmsg);
            }
		}); 
		$.ajax({
 			type: "PUT",
 			url: "/otscfgsvr/api/index/" + index_view_tablename + "/" + index_view_indexname + "/display_columns",
 			contentType: "application/json",
 			data: "{" + index_view_columns.join(",") + "}",
 			dataType: "text",
 			timeout:30000,
            success:function(msg){
            	
            },
            error: function(msg){
            	var errmsg = "保存显示列信息错误: " + getStructMsg(msg);
            	indexViewAlertMsg(errmsg);
            }
       	});
	} */
	
	function indexViewColumnsInit() {
			for (var j=0; j<index_view_query_columns.length; j++) {
	     		var columnId = "indexQueryColumns" + (j + 1);
	     		$("#" + columnId).attr("value", index_view_query_columns[j]);
	     		document.getElementById(columnId).title = index_view_query_columns[j];
	     	}
	     	for (var i=index_view_query_columns.length; i<5; i++){
	        	var columnNoId = "indexQueryColumns" + (i + 1);
	        	$("#" + columnNoId).attr("value", "");
	        	document.getElementById(columnNoId).disabled = "disabled";
	     	}
	}
	
	function indexViewUpdate(){
		if ($("#indexViewLimit").val()){
			return;
		}
		index_view_total_record_num += index_view_skip;
     	if(index_view_query.indexOf("&offset=") > 0) {
     		index_view_query = index_view_query.substring(0,index_view_query.indexOf("&offset="));
     	}

		$.ajax({
 			cache: false,
  			type: "GET",
  			url: "/otscfgsvr/api/index/" + index_view_tablename + "/" + index_view_indexname + "?" + index_view_query + "&offset=" + index_view_total_record_num, 
  			dataType: "json",
  			timeout: 30000,
  			success: function(results, msg){
  				index_view_records = results["records"];
  				for (var i=0; i<index_view_records.length; i++){
 					$.each(index_view_records[i],function(j){  
 						index_view_records[i][j] = htmlEscape(index_view_records[i][j]);  
 					});  
 				}
  				$('#indexView').bootstrapTable('append', index_view_records);
  			},
       		error: function(msg){
             	var errmsg = "查询表" + getStructName(index_view_tablename) + " 记录信息失败！错误: " + getStructMsg(msg);
               	indexViewAlertMsg(errmsg);
        	}
  		});
  	}
	
	function indexViewQuery() {
		document.getElementById("limitUnvalied").style.display = 'none';
		document.getElementById("skipUnvalied").style.display = 'none';
		document.getElementById("skipTooLarge").style.display = 'none';
		index_view_query = "";
		if (index_view_index_type == 1) {
			if ($("#indexQueryContent").val()) {
				index_view_query += "query=" + encodeURIComponent($("#indexQueryContent").val()).replace(/%20/g, "+");
			}
			else {
				index_view_query += "query=*:*";
			}
			if ($("#indexViewHashKey").val().trim()) {
				index_view_query += "&hash_key=" + encodeURIComponent($("#indexViewHashKey").val().trim());
			}
			if ($("#indexViewRangeKeyStart").val().trim()) {
				index_view_query += "&range_key_start=" + encodeURIComponent($("#indexViewRangeKeyStart").val().trim());
			}
			if ($("#indexViewRangeKeyEnd").val().trim()) {
				index_view_query += "&range_key_end=" + encodeURIComponent($("#indexViewRangeKeyEnd").val().trim());
			}
		}
		else {
			if ($("#indexQueryContent").val()) {
				index_view_query += "query=" + encodeURIComponent($("#indexQueryContent").val()).replace(/%20/g, "+");
			}
			else {
				index_view_query += "query=*:*";	
			}
			if ($("#indexViewFilters").val()){
				index_view_query += "&filters=" + encodeURIComponent($("#indexViewFilters").val());
			}
			if ($("#indexViewSort").val()) {
				index_view_query += "&orders=" + encodeURIComponent($("#indexViewSort").val());
			}
		}
		
		if (index_view_query_columns.length == 0 || index_view_query_columns[0].length == 0){
			/* index_view_query += "&columns=hash_key";
			if (index_view_pktype == 1) {
				index_view_query += ",range_key";
			} */
		}
		else {
			index_view_query += "&columns=" + index_view_query_columns.join(",");
		}

    	for (var i=index_view_query_columns.length; i<5; i++) {
    		index_view_query_columns[i] = "";
    	}
		var rex_check_str = /^\d+$/;		
		var indexViewLimit = $("#indexViewLimit").val().trim();
		if(indexViewLimit){
			if(!rex_check_str.test(indexViewLimit)  || indexViewLimit >10000 ){
				document.getElementById("limitUnvalied").style.display = 'block';
				return;
			}
			index_view_query +=  "&limit=" + parseInt(indexViewLimit, 10);
		}
		var indexViewSkip =$("#indexViewSkip").val().trim(); 
		if(indexViewSkip){
			if (!rex_check_str.test(indexViewSkip)) {
				document.getElementById("skipUnvalied").style.display = 'block';
				document.getElementById("skipTooLarge").style.display = 'none';
				return;
			}
			else if(isGtMax(indexViewSkip)) {
				document.getElementById("skipUnvalied").style.display = 'none';
				document.getElementById("skipTooLarge").style.display = 'block';
				return;
			}
			index_view_skip = parseInt(indexViewSkip, 10);
		}
		else {
			index_view_skip = 0;
		}
		index_view_query += "&offset=" + index_view_skip;
		$.ajax({
 			cache: false,
 			type: "GET",
  			url: "/otscfgsvr/api/index/" + index_view_tablename + "/" + index_view_indexname + "?" + index_view_query,
  			dataType: "json",
  			timeout: 30000,
            success: function(results, msg){
            	index_view_records = results["records"];
            	for (var i=0; i<index_view_records.length; i++){
     					$.each(index_view_records[i],function(j){  
     						index_view_records[i][j] = htmlEscape(index_view_records[i][j]);  
     					});  
     				}
         	},
			complete: function()
		    {
				var $table = $('#indexView').bootstrapTable();
     			var pagesize = $table.bootstrapTable('getOptions').pageSize;
				$('#indexView').bootstrapTable('destroy').bootstrapTable({
	                data: index_view_records, 
	                classes: 'table',
	                undefinedText: '',
	                striped: true,
	                pagination: true,
	                pageSize: pagesize,
	                pageList: [5, 10, 15],
	                formatRecordsPerPage: function (pageNumber) {
	                    return sprintf('每页显示 %s 条记录', pageNumber);
	                },
	               	formatShowingRows: function (pageFrom, pageTo, totalRows) {
	                    return sprintf('第  %s 到 %s 条记录', pageFrom, pageTo);
	                },
	                formatNoMatches: function () {
	                	return '没有匹配的记录。';
	                },
	                columns: index_view_show_columns
	            });
		    },
            error: function(msg){       		
             	var errmsg = "查询表 " + getStructName(index_view_tablename) + " 记录信息失败！错误: " + getStructMsg(msg);
               	indexViewAlertMsg(errmsg);
            }
		});     	
     }
	
	function editIndexViewColumns() {
		index_view_modal_table_name = index_view_tablename;
		index_view_modal_index_name = index_view_indexname;
	   $('#indexViewColumnEdit').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
	   $("#indexViewColumnEdit").modal({
		   backdrop: "static",
			show: false,
			remote: "index_view_column_edit.jsp"
		}); 
  	}
	
	 function goRecordDetail(row){
		 var queryUrl = "/otscfgsvr/api/record/" + index_view_tablename + "?query_from=0&hash_key=" + encodeURIComponent(htmlUnescape(row["hash_key"]));
		 if (index_view_pktype == 1) {
			 queryUrl += "&range_key=" + encodeURIComponent(htmlUnescape(row["range_key"]));
		 }
		$("#indexViewContent").attr("value", "");
		var indexViewContentHtml = "";
    	$.ajax({
 			cache: false,
    		type: "GET",
    		url: queryUrl,
    		dataType: "json",
    		timeout: 60000,
    		success: function(results, msg){
    			recordShownKeys = [];
    			indexViewContentHtml = "{\n\"records\":\n\t[\n\t\t{\n";
    			if (results["records"].length > 0) {
    				recordToShow = results["records"][0];
    			}
    			for(var item in recordToShow){
    				recordShownKeys.push(item);
    			}
    			for (var i=0; i<recordShownKeys.length; i++) {
    				indexViewContentHtml += "\t\t\"" + recordShownKeys[i] + "\":\"" + recordToShow[recordShownKeys[i]] + "\"";
    				if (i < recordShownKeys.length - 1){
    					indexViewContentHtml += ",\n";
    				}
    			}
    			indexViewContentHtml += "\n\t\t}\n\t]\n}";
    	    	indexViewhelpInfo(indexViewContentHtml);
    		},
    		error: function (msg){
             	var errmsg = "获取记录信息失败！错误: " + getStructMsg(msg);
               	indexViewhelpInfo(errmsg);
    		}
    	});
	 }
	 
	 /***********
	     function recordViewDelete(){
	      	var rowNum=0;
			var strs=document.getElementsByName("indexViewCheck");
			var rowSelectNum = 0;
			var rowDeletedNum = 0;
			var rowCheckNum = strs.length;
			var rowCheckValue = [];
			var rowSelectCheck = [];
			var rowDeleted = [];
			if(strs!=null && rowCheckNum>0){
				for(var i=0; i<rowCheckNum; i++){
					rowSelectCheck[i] = strs[i].checked;
					rowCheckValue[i] = strs[i].value;
					if(rowSelectCheck[i] == true){
						rowSelectNum++;
					}
				}
	 			if (rowSelectNum == 0){
	 				indexViewAlertMsg("请选择一个或多个索引。");	 				
					return;
				} 
				else {
					if(window.confirm('确定删除记录？')){
			            //alert("确定");
						for(var i=0; i<rowCheckNum; i++){
							if(rowSelectCheck[i]==true){
								
								document.body.style.cursor="wait";
						     	document.getElementById("btn_queryindexrec").style.cursor="wait";

								$.ajax({
									async: false,
									type:"DELETE",
					 				url:"/otscfgsvr/api/record/" + index_view_tablename + "?query_from=0&key=" + rowCheckValue[i],
					 				timeout:3000,
				                	success:function(msg){
				                		rowDeletedNum++;
				                		rowDeleted[i] = "#" + rowCheckValue[i];
				                	},
				   		            complete: function()
				   		            {
				   		         		document.body.style.cursor="default";
				   				     	document.getElementById("btn_queryindexrec").style.cursor="pointer";
				   		            },
				                	error: function(msg){
				    	             	var errmsg = "删除记录" + rowCheckValue[i] + " 错误: " + msg["status"];
				    	               	if (String(msg["status"]).indexOf("17") != 0)
				    	               		errmsg += " - " + msg["statusText"];    	               	
				    	               	errmsg += ". " + msg["responseText"];
				    	               	if(msg["status"] == 401)
				    	               		errmsg += "<br/>(请刷新页面或者重新登录！)";
				    	               	indexViewAlertMsg(errmsg);
					                }
								}); 
							}
		               	$(rowDeleted[i]).remove();
					}
				}
			    else{
	                 //alert("取消");
	                 return false;
	            }
			}
		}
	 } 
	 *******************/
	 
	 function getIndexViewShowColumns() {
	 	var propertyShowNum = 1;
     	if (index_view_pktype == 1) {
     		propertyShowNum = 2;
     	}
     	for (var i=0; i<index_view_query_columns.length; i++) {
     		var columnMap = {"align":"center", "sortable":true};
     		columnMap["field"] = index_view_query_columns[i];
     		columnMap["title"] = htmlEscape(index_view_query_columns[i]);
     		index_view_show_columns[propertyShowNum] = columnMap;
     		propertyShowNum++;
     	}
     	for (var i=propertyShowNum; i<index_view_show_columns_num; i++) {
     		index_view_show_columns[i] = index_view_column_property;
     	}
	 }
</script>
</html>
