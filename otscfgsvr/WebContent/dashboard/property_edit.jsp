<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">编辑显示列</h4>
</div>
<div class="modal-body">
	<div class="row" style="height:35px;">
        <div class="r_text" style="width:70px;padding-top:5px;">属性名：</div>
        <div class="r_box" style="width:450px;"><input name="" id="columnName" type="text" class="textbox1" style="width:200px;height:25px;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<input name="" type="button" class="btn2" style="width:60px" value="添加" onclick='columnAdd();'></div>
    </div>
    <div class="row" style="margin-left:20px;"> 
	      <div style="text-align:left;color:#428BCA;font-size:12px;">注：请确保记录中至少包含一列显示列，否则可能造成表数据加载失败！</div>
	</div>
   <div  class="tablebg1" style="height:35px;width:500px;margin-left:20px;margin-top:0px;">
        <div class="text_center" style="padding-top:10px">已添加属性（最多显示5列）</div>     
   </div>
   <div  class="tablebg2" style="width:500px;height:175px;margin-left:20px;">
        <!-- <div style="margin-left:10px;height:160px;text-align:left;" id="columnList"> -->
        <table id="columnList" data-show-header="false" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
      		<thead>
      			<tr>
      				<th data-field="state" data-checkbox="true"></th>
					<th data-field="property" data-align="center"></th>
					<th data-field="" data-align="center"></th>
					<th class="col-operate-property" data-field="operate" data-align="right" data-formatter="propertyOperateFormatter" data-events="operateEvents"></th>
				</tr>
      		</thead>
      	</table>
        <!-- </div> -->  
        <!-- <div class="row" style="margin-left:10px;margin-top:10px;">
        	<input name="" type="button" class="btn2" style="width:60px" value="向上" onclick="goUp();">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<input name="" type="button" class="btn2" style="width:60px" value="向下" onclick='goDown();'></div> -->
    </div>
   <div class="row" style="margin-left:20px;height:20px;margin-top:5px;"><div class="r_text14" id="columnEditStatus" style="width:500px;text-align:center"></div></div>  
   <div class="row" style="margin-top:0px;">
   	<div class="cy_text">
   		<input id="btn_editproperty" name="" type="button" class="btn2" style="width:60px" value="确定" onclick='columnConfirm();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
   		<input id="btn_cancelproperty" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
   	</div>
   </div>		
</div>

<script type="text/javascript">
	var columns = [];
	var propertyToAdd = [];
	var propertyToShow = [];
	
	$(function(){
		$('#columnList').bootstrapTable({
	        data: [],
	        classes: 'table-condensed',
            undefinedText: '',
            formatNoMatches: function () {
            	return '';
            }
	    });
		columnEditWindow();
	});
	
	function propertyOperateFormatter(value, row, index) {
        return [
                '<input name="" type="button" class="propertyDelete btn3" style="width:50px;" value="移除" data-method="remove">',
        ];
    }
	
	 window.operateEvents = {
             'click .propertyDelete': function (e, value, row, index) {
             	columnDelete(row);
             },
             'click .recordEdit': function (e, value, row, index) {
             	clickEditRecord(row);
             },
             'click .recordDelete': function (e, value, row, index) {
             	clickDeleteRecord(row);
             },
             'click .recordMultiDelete': function (e, value, row, index) {
              	deleteMultiRecord();
              }
         };
	
	function columnEditWindow(){
	 		$("#columnName").attr("value", "");
	 		$("#columnEditStatus").html("");  		
	 		$.ajax({
	 			cache: false,
	 			type: "GET",
	 			url: "/otscfgsvr/api/table/" + modal_table_name + "/display_columns",
	 			dataType: "json",
	 			timeout:30000,
	            success:function(results, msg){  
	            	if (results["errcode"] == 0) {
	            		var property = results["columns"];
		            	var showPosition = property.indexOf("\"propertyToShow\"");
		            	if (property.substring(17, showPosition-2).length == 0){
		            		propertyToAdd = [];
		            	}
		            	else {
			               	propertyToAdd = property.substring(17, showPosition-2).split(",");
		            	}
		            	if (property.substring(showPosition+18, property.length-1).length == 0){
		            		propertyToShow = [];
		            	}
		            	else {
			               	propertyToShow = property.substring(showPosition+18, property.length-1).split(",");
		            	}
		               	columns = 
		               		$.map(propertyToAdd, function(item){
		               			return {"property": htmlEscape(item)};
		               		});
		               	for(var i=0; i<columns.length; i++){
		               		for (var j=0; j<propertyToShow.length; j++){
		               			if (columns[i]["property"] == htmlEscape(propertyToShow[j])){
		               				columns[i] = {"property": columns[i]["property"], "state": true};
		               			}
		               		}
		               	}
		               	$('#columnList').bootstrapTable('destroy').bootstrapTable({
		        	        data: columns,
		        	        classes: 'table-condensed',
			                undefinedText: '',
			                formatNoMatches: function () {
			                	return '';
			                }
		        	    });
	            	}
	            	else {
	            		$("#columnEditStatus").append("获取显示列信息错误: " + results["errcode"]);
	            	}
	            },
	            error: function(msg){
	            	if (msg["status"] != 404){
	                	var errmsg = "获取显示列信息错误: " + getStructMsg(msg);
	                	$("#columnEditStatus").append(errmsg);
	            	}
	            }
       		});
	 	}
	 	
	 	function columnAdd(){
	 		var column_name = $("#columnName").val();
	 		if (!column_name){
	 			return;
	 		}
	 		$("#columnEditStatus").html("");
	 		if (column_name == "hash_key" || column_name == "range_key") {
	 			$("#columnEditStatus").append("hash_key和range_key为不可编辑属性。");
	 			return;
	 		}
	 		for (var i=0; i<columns.length; i++){
	 			if (column_name == columns[i]["property"]){
	 				$("#columnEditStatus").append("该列已添加。");
		 			return;
	 			}
	 		}
	 		if (columns.length >= 5){
	 			$("#columnEditStatus").append("最多展示5列。");
	 			return;
	 		}	
	 		columns.push({"property": htmlEscape(column_name),"state": true});
	 		$('#columnList').bootstrapTable('destroy').bootstrapTable({
    	        data: columns,
    	        classes: 'table-condensed',
                undefinedText: '',
                formatNoMatches: function () {
                	return '';
                }
    	    });
		}
		
		function columnDelete(editRow) {
			$("#columnEditStatus").html("");
			var dataArray = [];
        	var valuesArray = [];
        	dataArray[0] = editRow;
        	valuesArray[0] = editRow["property"];
        	$table = $('#columnList').bootstrapTable({
                data: dataArray
            });
			$table.bootstrapTable('remove', {
                field: 'property',
                values: valuesArray
            }); 
		}
		
		/* function doOrder(seq){
			var obj = document.getElementsByName("columnCheck");
			var column = [];
			var j = 0;
			for (var i=0; i<obj.length; i++){
				column[i] = obj[i].value
				if (obj[i].checked == true){
					var columnName = column[i];
					var columnNum = i;
					j++;
				}	
		}
			if (j == 0 || j > 1){
				return;
			}
			if (seq == -1){
			var	columnInsNum = columnNum - 1;
		}
		else if(seq == 1){
			var	columnInsNum = columnNum + 1;
		}
			if(columnInsNum < 0 || columnInsNum >= obj.length){
				return;
			}
		var columnId = "#" + columnName;
			$(columnId).remove();
			var columnInsert = "#" + column[columnInsNum];
			var columnInsertHtml = "<div style='margin-top:6px;' id='" + columnName + "'><input name='columnCheck' type='checkbox' value='" + columnName + "'>" + columnName + "</div>"; 			
			if (seq == -1){
				$(columnInsert).before(columnInsertHtml);
			}
		else if(seq == 1){
			$(columnInsert).after(columnInsertHtml);
		} 	
			var columnCheckBox = columnId + " input[type=checkbox]";
			$(columnCheckBox).attr("checked", true);	
		}
		
		function goUp()
		{
		   doOrder(-1); 		//向上移动的方法
		}
		
		function goDown()
		{
		   doOrder(1);    		//向下移动的方法
		} */
		
		function columnConfirm(){
			propertyToAdd  = [];
			$("#columnEditStatus").html("");
			var $table = $('#columnList').bootstrapTable();
			var selects = $table.bootstrapTable('getSelections');
			propertyToShow = $.map(selects, function (row) {
                return htmlUnescape(row.property);
            });
			for (var i=0; i<columns.length; i++) {
				propertyToAdd.push(htmlUnescape(columns[i]["property"]));
			} 		
			var property_data = "\"propertyToAdd\":\"" + propertyToAdd.join(",") + "\",\"propertyToShow\":\"" + propertyToShow.join(",") + "\"";
			$.ajax({
	 			type: "PUT",
	 			url: "/otscfgsvr/api/table/" + modal_table_name + "/display_columns",
	 			contentType: "application/json",
	 			data: JSON.stringify({"columns": property_data}),
	 			dataType: "json",
	 			timeout:30000,
	            success:function(results, msg){
	            	if (results["errcode"] == 0) {
	            		table_detail_query_columns = propertyToShow;
	            		getTableDetailShowColumns();
		                getRecordList();
			 			$("#columnEditStatus").append("编辑显示列成功。");
	            	}
	            	else {
	            		$("#columnEditStatus").append("保存显示列信息失败！错误: " + results["errcode"]);
	            	}
	            },
	            error: function(msg){
                	var errmsg = "保存显示列信息失败！错误: " + getStructMsg(msg);
                	$("#columnEditStatus").append(errmsg);
	            }
	       	});			
		}
</script>
