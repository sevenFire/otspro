<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">编辑索引</h4>
</div>
<div class="modal-body">
<div>
   <div  class="row" style="margin-top:0px;">
     <div class="r_text" style="width:90px;margin-top:0px;padding-top:3px;">*索引名：</div>
     <div class="r_box" style="width:145px;margin-top:0px;"><input name="indexNameEdit" id="indexNameEdit" type="text" class="textbox1" style="width:145px;height:25px;background-color:#EBEBE4;" disabled="disabled"></div>
   	 <div class="r_text14" style="width:40px;padding-top:3px;"></div>
  </div>
  <div class="row">
  		<div class="r_text" style="margin-top:0px;width:90px;padding-top:3px;">*类型：</div>
  		<div class="r_box" style="margin-top:0px;">
			<select name="" id="indexTypeEdit" style="width:145px;height:25px;background-color:#EBEBE4;" disabled="disabled">
				<option value=0 selected>基于Solr</option>
				<option value=1>基于Hbase</option>
			</select>	
		</div>
  </div>
   <div  class="row" id="indexEditMode" style="margin-top:3px;">
     <div class="r_text" style="width:90px;margin-top:5px;">模式：</div>
     <div class="r_box"  style="width:160px;margin-top:5px;">
     	<input name="indexPatternEdit" id="onPatternEdit" type="radio" value="1" checked><span>实时索引</span>
     	<input name="indexPatternEdit" id="offPatternEdit" type="radio" value="0"><span>离线索引</span>
     </div>
     <div class="r_text" style="margin-top:3px;width:80px;">*分片数：</div>
     <div class="r_box" style="width:40px;margin-top:3px;"><input name="indexShardEdit" id="indexShardEdit" type="text" class="textbox1" style="width:40px;height:25px;background-color:#EBEBE4;" disabled="disabled"></div>
     <div class="r_box" style="width:10px;margin-top:3px;"><span class="glyphicon glyphicon-question-sign" title="建议：分片数与集群内Solr服务数一致"></span></div>
  </div>
   <!-- <div id="hashKeyRowIndexEdit" class="row" style="margin-top:5px;display:none;">
   	 <div class="r_text" style="width:90px;margin-top:5px;">Hash键：</div>
     <div class="r_box"  style="width:160px;margin-top:0px;">
     	<input name="" id="hashKeyIndexEdit" type="text" class="" style="width:160px;height:25px;" value="" placeholder="Hash键">&nbsp;&nbsp;&nbsp;
     </div>
   </div>
   <div id="rangeKeyRowIndexEdit" class="row" style="margin-top:5px;display:none;">
   	 <div class="r_text" style="width:90px;margin-top:5px;">Range键：</div>
     <div class="r_box"  style="width:350px;margin-top:0px;">
     	<input name="" id="startKeyIndexEdit" type="text" class="" style="width:160px;height:25px;" value="" placeholder="起始键">&nbsp;&nbsp;&nbsp;
     	<input name="" id="endKeyIndexEdit" type="text" class="" style="width:160px;height:25px;" value="" placeholder="终止键">
     </div>
   </div> -->
   <!-- <div  class="row" style="margin-left:30px;margin-top:0px;">
     <div class="r_box" style="width:200px;margin-top:0px;"><div class="r_text" style="width:200px;;margin-left:0px;margin-top:0px;padding-top:3px;text-align:center;">*列名</div></div>
     <div class="r_box" style="width:120px;margin-top:0px;"><div class="r_text" style="width:110px;;margin-left:0px;margin-top:0px;padding-top:3px;text-align:center;">类型</div></div>
   </div> -->
	<div class="row" id="indexEditSolrColumns" style="margin-top:0px;overflow-y:auto;max-height:300px;">
	   <div class="tablebg2" id="indexEditSolrColumn1" style="width:430px;height:40px;margin-left:30px;margin-bottom:5px;">
		   	 <div class="r_text" style="width:60px;padding-top:3px;">*列名：</div>
		   	 <div class="r_box" style="width:145px;"><input name="indexSolrColumnName" id="indexEditSolrColumnName" type="text" class="textbox1" style="width:145px;height:25px;"></div>
		   	 <div class="r_box" style="width:110px;">
		   		<select name="" id="indexEditSolrColumnType" style="width:110px;height:25px;">
		   			<option value="string" selected>string</option>
		   			<option value="boolean">boolean</option>
		   			<option value="int32">int32</option>   								
		   			<option value="int64">int64</option>
		   			<option value="float32">float32</option>
		   			<option value="float64">float64</option>
		   			<option value="binary" disabled="disabled">binary</option>   								
		   			<option value="location">location</option>
		   			<option value="location_rpt">location_rpt</option>
		   		</select>
		   	 </div>
		   	 <div class="r_box" style="width:10px;padding-top:3px;text-align:right;"><a href="#"><span class="glyphicon glyphicon-plus" title="增加列" onclick='addIndexEditSolrColumn();'></span></a></div>
	  </div>
   		<div class="row" id="indexEditSolrLastColumn" style="display:none"></div>
	</div>
	<div class="row" id="indexEditHbaseColumns" style="margin-top:0px;overflow-y:auto;max-height:300px;display:none;">
	   <div class="tablebg2" id="indexEditHbaseColumn1" style="width:430px;height:40px;margin-left:30px;margin-bottom:5px;">
		   	 <div class="r_text" style="width:60px;padding-top:3px;">*列名：</div>
		   	 <div class="r_box" style="width:145px;"><input name="" id="indexEditHbaseColumnName" type="text" class="textbox1" style="width:145px;height:25px;"></div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		   	 <div class="r_box" style="width:80px;">
		   		<select name="" id="indexEditHbaseColumnType" style="width:80px;height:25px;" onchange="indexHbaseColumnTypeSelect(1);">
		   			<option value="string" selected>string</option>
		   			<option value="int32">int32</option>   								
		   			<option value="int64">int64</option>
		   			<option value="float32">float32</option>
		   			<option value="float64">float64</option>
		   			<option value="binary">binary</option> 
		   		</select>
		   	 </div>
		   	 <div class="r_box" style="width:60px;"><input name="" id="indexEditHbaseColumnMaxLength" type="text" class="textbox1" style="width:60px;height:25px;" placeholder="最大长度"></div>&nbsp;&nbsp;
		</div>
   		<div class="row" id="indexEditHbaseLastColumn" style="display:none"></div>
	</div>
   <div class="row" style="margin-top:0px;height:20px;"><div class="r_text14" id="indexUpdateStatus" style="width:500px;text-align:center"></div></div>
   <div  class="row" style="margin-left:170px;margin-top:0px;">
     <div class="r_box">
     	<input id="btn_editindex" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='indexUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
     	<input id="btn_canceledit" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
     </div>
   </div>
</div> 	
</div>

    <script type="text/javascript">
    	var indexEditInfoInit = modal_index_info;
    	var indexEditColumnId = 2;
    	var indexTypes = ["string", "boolean", "int32", "int64", "float32", "float64", "binary", "location", "location_rpt"];
		
    	$(function(){
    		/* if (args_pktype == 1) {
    			//document.getElementById("hashKeyRowIndexEdit").style.display = "block";
    			//document.getElementById("rangeKeyRowIndexEdit").style.display = "block";
    		} */
    		indexUpdateWindow(indexEditInfoInit);
    		$("input[type='text']").focus(function() {
    			$("#indexUpdateStatus").html("");
        	});
    	});
    	
    	/* function indexPatternCheck(operation, pattern){
 			if (operation == 1){
 	    		if (pattern == 1){
 	    			$("#endKeyIndexEdit").attr("disabled", true);
 	    		}
 	    		else {
 	    			$("#endKeyIndexEdit").attr("disabled", false);
 	    		}
 			}
    	} */
    	
    	function addIndexEditSolrColumn() {
    		var text = '<div class="tablebg2" id="indexEditSolrColumn' + indexEditColumnId + '" style="width:430px;height:40px;margin-left:30px;margin-bottom:5px;">';
    		text += document.getElementById("indexEditSolrColumn1").innerHTML;
    		text += '<div class="r_box" style="width:10px;padding-top:3px;"><a href="#"><span class="glyphicon glyphicon-minus" title="删除列" onclick="removeIndexEditSolrColumn(' + indexEditColumnId + ');"></span></a></div>';
    		text += '</div>';
    		$("#indexEditSolrColumns").find("#indexEditSolrLastColumn").before(text);
    		indexEditColumnId++;
    		$("input[type='text']").focus(function() {
    			$("#indexUpdateStatus").html("");
        	});
    	}
    	
    	function removeIndexEditSolrColumn(id) {
    		var removeId = "#indexEditSolrColumn" + id;
    		$(removeId).remove();
    	}
 		
 		function indexUpdateWindow(indexEditInfoInit){	
 			$("#indexUpdateStatus").html("");
			$("#indexNameEdit").attr("value", indexEditInfoInit["index_name"]);
			$("#indexTypeEdit").attr("value", indexEditInfoInit["type"]);
 			//$("#startKeyIndexEdit").attr("value", "");
			//$("#endKeyIndexEdit").attr("value", "");
			if (indexEditInfoInit["type"] == 1) {
				document.getElementById("indexEditMode").style.display = "none";
 				document.getElementById("indexEditSolrColumns").style.display = "none";
 				document.getElementById("indexEditHbaseColumns").style.display = "block";
 				
 				var i = 0;
				$("#indexEditHbaseColumn1").find("#indexEditHbaseColumnName").val(indexEditInfoInit["columns"][i]["column"]);
        		$("#indexEditHbaseColumn1").find("#indexEditHbaseColumnType").val(indexEditInfoInit["columns"][i]["type"]);
        		if (indexEditInfoInit["columns"][i]["type"] == "string" || indexEditInfoInit["columns"][i]["type"] == "binary") {
        			$("#indexEditHbaseColumn1").find("#indexEditHbaseColumnMaxLength").val(indexEditInfoInit["columns"][i]["maxLen"]);
	    		}
        		else {
	    			$("#indexEditHbaseColumn1").find("#indexEditHbaseColumnMaxLength").css("display", "none");
	    		}
				for(i=1; i<indexEditInfoInit["columns"].length; i++){
					var text = '<div class="tablebg2" id="indexEditHbaseColumn' + indexEditColumnId + '" style="width:430px;height:40px;margin-left:30px;margin-bottom:5px;">';
		    		text += document.getElementById("indexEditHbaseColumn1").innerHTML;
		    		text += '</div>';
		    		$("#indexEditHbaseColumns").find("#indexEditHbaseLastColumn").before(text);
		    		$("#indexEditHbaseColumn" + indexEditColumnId).find("#indexEditHbaseColumnName").val(indexEditInfoInit["columns"][i]["column"]);
		    		$("#indexEditHbaseColumn" + indexEditColumnId).find("#indexEditHbaseColumnType").val(indexEditInfoInit["columns"][i]["type"]);
		    		if (indexEditInfoInit["columns"][i]["type"] == "string" || indexEditInfoInit["columns"][i]["type"] == "binary") {
		    			$("#indexEditHbaseColumn" + indexEditColumnId).find("#indexEditHbaseColumnMaxLength").css("display", "block");
		    			$("#indexEditHbaseColumn" + indexEditColumnId).find("#indexEditHbaseColumnMaxLength").val(indexEditInfoInit["columns"][i]["maxLen"]);
		    		}
		    		else {
		    			$("#indexEditHbaseColumn" + indexEditColumnId).find("#indexEditHbaseColumnMaxLength").css("display", "none");
		    		}
		    		indexEditColumnId++;
				}
				$("#indexEditHbaseColumns").find("input").attr("disabled","disabled");
				$("#indexEditHbaseColumns").find("input").css("background-color", "#EBEBE4");
				$("#indexEditHbaseColumns").find("select").attr("disabled","disabled");
				$("#indexEditHbaseColumns").find("select").css("background-color", "#EBEBE4");
	    		$("#indexUpdateStatus").html("基于Hbase索引不可编辑。");
	    		document.getElementById("btn_editindex").style.display = "none";
			}
			else {
				document.getElementById("indexEditMode").style.display = "block";
 				document.getElementById("indexEditSolrColumns").style.display = "block";
 				document.getElementById("indexEditHbaseColumns").style.display = "none";
 				if (indexEditInfoInit["pattern"] == 0){
            		$("#offPatternEdit").attr("checked", true);
            	}
            	else if (indexEditInfoInit["pattern"] == 1){
            		$("#onPatternEdit").attr("checked", true);
            	}
 				$("input[name='indexShardEdit']:text").attr("value", indexEditInfoInit["shard_num"]);
 				
 				var i = 0;
				$("#indexEditSolrColumn1").find("#indexEditSolrColumnName").val(indexEditInfoInit["columns"][i]["column"]);
        		$("#indexEditSolrColumn1").find("#indexEditSolrColumnType").val(indexEditInfoInit["columns"][i]["type"]);
				for(i=1; i<indexEditInfoInit["columns"].length; i++){
					var text = '<div class="tablebg2" id="indexEditSolrColumn' + indexEditColumnId + '" style="width:430px;height:40px;margin-left:30px;margin-bottom:5px;">';
		    		text += document.getElementById("indexEditSolrColumn1").innerHTML;
		    		text += '<div class="r_box" style="width:10px;padding-top:3px;"><a href="#"><span class="glyphicon glyphicon-minus" title="删除列" onclick="removeIndexEditSolrColumn(' + indexEditColumnId + ');"></span></a></div>';
		    		text += '</div>';
		    		$("#indexEditSolrColumns").find("#indexEditSolrLastColumn").before(text);
		    		$("#indexEditSolrColumn" + indexEditColumnId).find("#indexEditSolrColumnName").val(indexEditInfoInit["columns"][i]["column"]);
		    		$("#indexEditSolrColumn" + indexEditColumnId).find("#indexEditSolrColumnType").val(indexEditInfoInit["columns"][i]["type"]);
		    		indexEditColumnId++;
		    		$("input[type='text']").focus(function() {
		    			$("#indexUpdateStatus").html("");
		        	});
				}
			}
 	 	}
		
		function indexUpdate(){
			$("#indexUpdateStatus").html("");

			var indexEditMap = {};
			indexEditMap["rebuildinfo"] = {};
			indexEditMap["rebuildinfo"]["shard_num"] = $("#indexShardEdit").val();
			indexEditMap["rebuildinfo"]["pattern"] = $("input[name='indexPatternEdit']:checked").val();
			/* if (args_type == 1) {
				if ($("#startKeyIndexEdit").val()){
					indexEditMap["rebuildinfo"]["start_key"] = encodeURIComponent($("#startKeyIndexEdit").val());
				}
				if ($("#endKeyIndexEdit").val()){
					indexEditMap["rebuildinfo"]["end_key"] = encodeURIComponent($("#endKeyIndexEdit").val());
				}
			} */
			
			//if ($("input[name='indexPatternEdit']:checked").val() == 0 && endKey == null){
			//	$("#indexUpdateStatus").append("离线索引必须提供终止键。");
			//	return;
			//}
			
     		var indexColumnInfos = [];
     		var indexColumns = [];
     		var k = 0;
     		$("#indexEditSolrColumns").children(".tablebg2").each(function() {
     			if ($(this).find("#indexEditSolrColumnName").val()){
     				indexColumnInfos[k] = {"column":$(this).find("#indexEditSolrColumnName").val(),"type":$(this).find("#indexEditSolrColumnType").val()};
     				indexColumns[k] = $(this).find("#indexEditSolrColumnName").val();
     				k++;
     			}
     		});
     		if (indexColumnInfos.length == 0){
     			$("#indexUpdateStatus").html("");
     			$("#indexUpdateStatus").append("请输入列名。");
     			return;
     		}
     		if (isRepeat(indexColumns)) {
     			$("#indexUpdateStatus").html("");
     			$("#indexUpdateStatus").append("列名不可重复。");
     			return;
     		}
			indexEditMap["rebuildinfo"]["columns"] = indexColumnInfos;
     		
				$.ajax({
					type:"PUT",
		 			url:"/otscfgsvr/api/index/" + modal_table_name + "/" + $("#indexNameEdit").val(),
		 			data: JSON.stringify(indexEditMap),
		 			dataType:"json",
		 			contentType: "application/json",
		 			timeout:100000,
		            success:function(results, msg) {            	
	                	if (results.errcode != 0) {
	                		$("#indexUpdateStatus").append("更新索引任务" + errorInfo(results["errcode"]));
	                	}
	                	else {
			            	$("#indexUpdateStatus").append("更新索引任务提交成功。");
			            	$.ajax({
			            		cache: false,
	        					type:"GET",
	        	 				url:"/otscfgsvr/api/index/" + args_tablename + "/" + $("#indexNameEdit").val() + "?query_from=0",
	        	 	 			dataType: "json",
	        	 				timeout:30000,
	        		            success:function(results, msg) {
	        		            	if(results["errcode"] != 0){
	        		            		recordAlertMsg("获取索引" + errorInfo(results["errcode"]) + "错误码: " + results["errcode"]);
	        		            	}
	        		            	results["progress"] = 0;
	        	                	var $table = $("#tableDetailIndex").bootstrapTable();
	        	                  	$table.bootstrapTable('updateRow', {
	        	                        index: table_detail_row_index,
	        	                        row: results,
	        	                    });
	                        	},
	                        	error: function(msg){
	                            	var errmsg = "获取索引 " + getStructName(indexname) + " 信息错误: " + getStructMsg(msg);
	                            	recordAlertMsg(errmsg);
	        	                }
	        				});
	                	}
		            },
		            error: function(msg){
	            		var errmsg = "更新索引任务提交失败！错误: " + getStructMsg(msg);
		               	$("#indexUpdateStatus").append(errmsg);
		            }
				});
		}
    </script>