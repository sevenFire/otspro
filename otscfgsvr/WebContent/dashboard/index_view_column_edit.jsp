<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">编辑显示列</h4>
</div>
<div class="modal-body">
	<!-- <div class="row" style="height:35px;">
        <div class="r_text" style="width:80px;padding-top:5px;">属性名：</div>
        <div class="r_box" style="width:450px;"><input name="" id="columnName" type="text" class="textbox1" style="width:200px;height:25px;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<input name="" type="button" class="btn2" style="width:60px" value="添加" onclick='columnAdd();'></div>
    </div> -->
   <div  class="tablebg1" style="height:35px;width:500px;margin-left:20px;margin-right:20px;">
        <div class="text_center" style="padding-top:10px">选择需显示的属性（最多显示5列）</div>     
   </div>
   <div  class="tablebg2" style="width:500px;height:200px;margin-left:20px;margin-right:20px;">
        <table id="indexViewColumnList" data-show-header="false" data-sort-name="property" data-height="200" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
      		<thead>
      			<tr>
      				<th data-field="state" data-checkbox="true"></th>
					<th data-field="property" data-align="center"></th>
					<th data-field="" data-align="center"></th>
					<th data-field="" data-align="center"></th>
				</tr>
      		</thead>
      	</table>
        <!-- </div> -->  
        <!-- <div class="row" style="margin-left:10px;margin-top:10px;">
        	<input name="" type="button" class="btn2" style="width:60px" value="向上" onclick="goUp();">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<input name="" type="button" class="btn2" style="width:60px" value="向下" onclick='goDown();'></div> -->
    </div>
   <div class="row" style="margin-left:20px;height:20px;margin-top:5px;"><div class="r_text14" id="indexViewColumnEditStatus" style="width:500px;text-align:center"></div></div>  
   <div class="row" style="margin-top:0px;">
   	<div class="cy_text">
   		<input id="btn_editproperty" name="" type="button" class="btn2" style="width:60px" value="确定" onclick='indexViewColumnConfirm();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
   		<input id="btn_cancelproperty" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
   	</div>
   </div>		
</div>

<script type="text/javascript">
	var columns = [];
	var columns_show = [];
	var propertyToAdd = [];
	var propertyToShow = [];
	
	$(function(){
		$('#indexViewColumnList').bootstrapTable({
	        data: [],
	        classes: 'table-condensed',
            undefinedText: '',
            formatNoMatches: function () {
            	return '';
            }
	    });
		indexViewColumnEditWindow();
	});
	
	function indexViewColumnEditWindow(){
	 		/* $("#columnName").attr("value", ""); */
	 		$("#indexViewColumnEditStatus").html("");		
	 		$.ajax({
	 			cache: false,
	 			async: false,
	 			type: "GET",
	 			url: "/otscfgsvr/api/index/" + index_view_modal_table_name + "/" + index_view_modal_index_name + "?query_from=0",
	 			dataType: "json",
	 			timeout:30000,
	            success:function(results, msg){  
	               	for(var i=0; i<results["columns"].length; i++){
	               		columns[i] = {"property": htmlEscape(results["columns"][i]["column"])};
	               	}
	            },
	            error: function(msg){
	                var errmsg = "获取显示列信息失败！错误: " + getStructMsg(msg);
	                $("#indexViewColumnEditStatus").append(errmsg);
	            }
       		});

           	$.ajax({
	 			cache: false,
	 			type: "GET",
	 			url: "/otscfgsvr/api/index/" + index_view_modal_table_name + "/" + index_view_modal_index_name + "/display_columns",
	 			dataType: "json",
	 			timeout:30000,
	            success:function(results, msg){  
	            	if (results["errcode"] == 0) {
	            		var property = results["columns"];
		            	if (property.length == 0){
		            		columns_show = [];
		            	}
		            	else {
		            		columns_show = property.split(",");
		            	}
		            	for(var i=0; i<columns.length; i++){
		               		for (var j=0; j<columns_show.length; j++){
		               			if (columns[i]["property"] == htmlEscape(columns_show[j])){
		               				columns[i] = {"property": columns[i]["property"], "state": true};
		               			}
		               		}
		               	}
	            	}
	            	else {
	            		$("#indexViewColumnEditStatus").append("获取显示列信息失败！错误: " + results["errcode"]);
	            	}
	            },
	            complete: function()
	            {
	               	$('#indexViewColumnList').bootstrapTable('destroy').bootstrapTable({
	        	        data: columns,
	        	        classes: 'table-condensed',
		                undefinedText: '',
		                formatNoMatches: function () {
		                	return '';
		                }
	        	    });
	            },
	            error: function(msg){
	            	if (msg["status"] != 404){
	                	var errmsg = "获取显示列信息失败！错误: " + getStructMsg(msg);
	                	$("#indexViewColumnEditStatus").append(errmsg);
	            	}
	            }
       		});
	 	}
		
		function indexViewColumnConfirm(){
			propertyToShow  = [];
			$("#indexViewColumnEditStatus").html("");
			var $table = $('#indexViewColumnList').bootstrapTable();
			var selects = $table.bootstrapTable('getSelections');
				propertyToShow = $.map(selects, function (row) {
                return htmlUnescape(row.property);
            });
			if (propertyToShow.length > 5) {
            	$("#indexViewColumnEditStatus").append("最多显示5列。");
            	return;
			}
			$.ajax({
	 			type: "PUT",
	 			url: "/otscfgsvr/api/index/" + index_view_modal_table_name + "/" + index_view_modal_index_name + "/display_columns",
	 			contentType: "application/json",
	 			data: JSON.stringify({"columns":propertyToShow.join(",")}),
	 			dataType: "json",
	 			timeout:30000,
	            success:function(msg){
		 			$("#indexViewColumnEditStatus").append("编辑显示列成功。");
		 			index_view_query_columns = propertyToShow;
		 			getIndexViewShowColumns();
		 			indexViewColumnRefresh();
			       	indexViewRecordRefresh();
	            },
	            error: function(msg){
                	var errmsg = "保存显示列信息错误: " + getStructMsg(msg);
                	$("#indexViewColumnEditStatus").append(errmsg);
	            }
	       	});
		}
		
		function indexViewColumnRefresh() {
			for (var j=0; j<propertyToShow.length; j++) {
         		var columnId = "indexQueryColumns" + (j + 1);
         		$("#" + columnId).attr("value", propertyToShow[j]);
         		document.getElementById(columnId).title = propertyToShow[j];
         		document.getElementById(columnId).removeAttribute("disabled");;
         	}
         	for (var i=propertyToShow.length; i<5; i++){
            	var columnNoId = "indexQueryColumns" + (i + 1);
            	$("#" + columnNoId).attr("value", "");
            	document.getElementById(columnNoId).disabled = "disabled";
         	}
		}
		
		function indexViewRecordRefresh(){	
   		            	var $table = $('#indexView').bootstrapTable();
   		     			var pagesize = $table.bootstrapTable('getOptions').pageSize;
   		            	$('#indexView').bootstrapTable('destroy').bootstrapTable({
   			                data: [], 
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
 			                	return '请输入查询条件。';
 			                },
 		                	columns: index_view_show_columns
   			            });
		}
</script>
