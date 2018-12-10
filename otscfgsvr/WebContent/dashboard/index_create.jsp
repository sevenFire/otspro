<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">创建索引</h4>
</div>
<div class="modal-body">
   	<div class="row" style="margin-top:0px;">
     	<div class="r_text" style="margin-top:0px;width:80px;padding-top:3px;">*索引名：</div>
     	<div class="r_box" style="width:145px;margin-top:0px;"><input name="" id="indexNameCreate" type="text" class="textbox1" style="width:145px;height:25px;"></div>
  	</div>
  	<div class="row">
  		<div class="r_text" style="margin-top:0px;width:80px;padding-top:3px;">*类型：</div>
  		<div class="r_box" style="margin-top:0px;">
			<select name="" id="indexCreateType" style="width:145px;height:25px;" onchange="indexCreateTypeSelect()">
				<option value=0 selected>基于Solr</option>
				<option value=1>基于Hbase</option>
			</select>	
		</div>
  	</div>
   <div class="row" id="indexCreateMode" style="margin-top:5px;">
     <div class="r_text" style="width:80px;margin-top:3px;">模式：</div>
     <div class="r_box"  style="width:160px;margin-top:3px">
     	<input name="indexPatternCreate" type="radio" value="1" checked><span>实时索引</span>
     	<input name="indexPatternCreate" type="radio" value="0"><span>离线索引</span>
     </div>
     <div class="r_text14" style="width:40px;padding-top:3px;"></div>
  	 <div class="r_text" style="margin-top:3px;width:80px;">*分片数：</div>
     <div class="r_box" style="width:40px;margin-top:3px;"><input name="" id="indexShardCreate" type="text" class="textbox1" style="width:40px;height:25px;" value="3"></div>
     <div class="r_box" style="width:10px;margin-top:3px;margin-left:0px;padding-top:3px;"><span class="glyphicon glyphicon-question-sign" title="建议：分片数与集群内Solr服务数一致"></span></div>
   </div>
   <!-- <div id="hashKeyRowIndexCreate" class="row" style="margin-top:5px;display:none;">
   	 <div class="r_text" style="width:90px;margin-top:5px;">Hash键：</div>
     <div class="r_box"  style="width:160px;margin-top:0px;">
     	<input name="" id="hashKeyIndexCreate" type="text" class="" style="width:160px;height:25px;" value="" placeholder="Hash键">&nbsp;&nbsp;&nbsp;
     </div>
   </div>
   <div id="rangeKeyRowIndexCreate" class="row" style="margin-top:5px;display:none;">
   	 <div class="r_text" style="width:90px;margin-top:5px;">Range键：</div>
     <div class="r_box"  style="width:350px;margin-top:0px;">
     	<input name="" id="startKeyIndexCreate" type="text" class="" style="width:160px;height:25px;" value="" placeholder="起始键">&nbsp;&nbsp;&nbsp;
     	<input name="" id="endKeyIndexCreate" type="text" class="" style="width:160px;height:25px;" value="" placeholder="终止键">
     </div>
   </div> -->
   <!-- <div  class="row" style="margin-left:30px;margin-top:0px;">
     <div class="r_box" style="width:200px;margin-top:0px;"><div class="r_text" style="width:200px;margin-left:0px;margin-top:0px;padding-top:3px;text-align:center;">*列名</div></div>
     <div class="r_box" style="width:120px;margin-top:0px;"><div class="r_text" style="width:110px;margin-left:0px;margin-top:0px;padding-top:3px;text-align:center;">类型</div></div>
   </div> -->
   <div class="row" id="indexCreateSolrColumns" style="margin-top:0px;overflow-y:auto;max-height:300px;">
	   <div class="tablebg2" id="indexCreateSolrColumn1" style="width:470px;height:40px;margin-left:20px;margin-bottom:5px;">
		   	 <div class="r_text" style="width:60px;padding-top:3px;">*列名：</div>
		   	 <div class="r_box" style="width:145px;"><input name="indexSolrColumnName" id="indexSolrColumnName" type="text" class="textbox1" style="width:145px;height:25px;"></div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		   	 <div class="r_box" style="width:110px;">
		   		<select name="" id="indexSolrColumnType" style="width:110px;height:25px;" onchange="clearStatusHtml()">
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
		   	 <div class="r_box" style="width:10px;padding-top:3px;text-align:right;"><a href="#"><span class="glyphicon glyphicon-plus" title="增加列" onclick='addIndexCreateSolrColumn();'></span></a></div>
	  </div>
   		<div class="row" id="indexSolrLastColumn" style="display:none"></div>
	</div>
	<div class="row" id="indexCreateHbaseColumns" style="margin-top:0px;overflow-y:auto;max-height:300px;display:none;">
	   <div class="tablebg2" id="indexCreateHbaseColumn1" style="width:470px;height:40px;margin-left:20px;margin-bottom:5px;">
		   	 <div class="r_text" style="width:60px;padding-top:3px;">*列名：</div>
		   	 <div class="r_box" style="width:145px;"><input name="indexHbaseColumnName" id="indexHbaseColumnName1" type="text" class="textbox1" style="width:145px;height:25px;"></div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		   	 <div class="r_box" style="width:80px;">
		   		<select name="indexHbaseColumnType" id="indexHbaseColumnType1" style="width:80px;height:25px;" onchange="indexHbaseColumnTypeSelect(1);">
		   			<option value="string" selected>string</option>
		   			<option value="int32">int32</option>   								
		   			<option value="int64">int64</option>
		   			<option value="float32">float32</option>
		   			<option value="float64">float64</option>
		   			<option value="binary" disabled="disabled">binary</option> 
		   		</select>
		   	 </div>
		   	 <div class="r_box" style="width:60px;"><input name="indexHbaseColumnMaxLength" id="indexHbaseColumnMaxLength1" type="text" class="textbox1" style="width:60px;height:25px;" placeholder="最大长度"></div>&nbsp;&nbsp;
		   	 <div class="r_box" style="width:10px;padding-top:3px;text-align:right;"><a href="#"><span class="glyphicon glyphicon-plus" title="增加列" onclick='addIndexCreateHbaseColumn();'></span></a></div>
	  </div>
   		<div class="row" id="indexHbaseLastColumn" style="display:none"></div>
	</div>
  	<div class="row" style="margin-top:0px;height:20px;"><div class="r_text14" id="indexCreateStatus" style="width:500px;text-align:center"></div></div>
   <div  class="row" style="margin-top:0px;margin-left:170px;">
     <div class="r_box">
     	<input id="btn_createindex"  name="" type="button" class="btn2" style="width:60px" value="创建" onclick='indexCreate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
     	<input id="btn_cancelindex" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
     </div>
   </div>
</div> 	

    <script type="text/javascript">		
    	var columnId = 2;

    	$(function(){
    		/* if (args_pktype == 1) {
    			//document.getElementById("hashKeyRowIndexCreate").style.display = "block";
    			//document.getElementById("rangeKeyRowIndexCreate").style.display = "block";
    		} */
    		indexCreateWindow();
    		$("input[type='text']").focus(function() {
    			$("#indexCreateStatus").html("");
        	});
    	});
    	
 		function indexCreateWindow(){
 			$("#indexCreateStatus").html("");
 			$("#indexNameCreate").attr("value", "");
 			//indexPatternCheck(0,1);
 			if ($("#indexCreateType").val() == 1) {
 				document.getElementById("indexCreateMode").style.display = "none";
 				document.getElementById("indexCreateSolrColumns").style.display = "none";
 				document.getElementById("indexCreateHbaseColumns").style.display = "block";
 			}
 			else if ($("#indexCreateType").val() == 0) {
 				document.getElementById("indexCreateMode").style.display = "block";
 				document.getElementById("indexCreateSolrColumns").style.display = "block";
 				document.getElementById("indexCreateHbaseColumns").style.display = "none";
 			}
 		}
    	
    	function addIndexCreateSolrColumn() {
    		var text = '<div class="tablebg2" id="indexCreateSolrColumn' + columnId + '" style="width:470px;height:40px;margin-left:20px;margin-bottom:5px;">';
    		text += document.getElementById("indexCreateSolrColumn1").innerHTML;
    		text += '<div class="r_box" style="width:10px;padding-top:3px;"><a href="#"><span class="glyphicon glyphicon-minus" title="删除列" onclick="removeIndexCreateSolrColumn(' + columnId + ');"></span></a></div>';
    		text += '</div>';
    		$("#indexSolrLastColumn").before(text);
    		columnId++;
    		$("input[type='text']").focus(function() {
    			$("#indexCreateStatus").html("");
        	});
    	}
    	
    	function removeIndexCreateSolrColumn(id) {
    		var removeId = "#indexCreateSolrColumn" + id;
    		$(removeId).remove();
    	}
    	
    	function addIndexCreateHbaseColumn() {
    		var text = '<div class="tablebg2" id="indexCreateHbaseColumn' + columnId + '" style="width:470px;height:40px;margin-left:20px;margin-bottom:5px;">';
    		
    		text += '<div class="r_text" style="width:60px;padding-top:3px;">*列名：</div>';
		   	text += '<div class="r_box" style="width:145px;"><input name="indexHbaseColumnName" id="indexHbaseColumnName' + columnId + '" type="text" class="textbox1" style="width:145px;height:25px;"></div>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;';
		   	text += '<div class="r_box" style="width:80px;">';
		   	text += '<select name="indexHbaseColumnType" id="indexHbaseColumnType' + columnId + '" style="width:80px;height:25px;" onchange="indexHbaseColumnTypeSelect(' + columnId + ');">';
		   	text += '<option value="string" selected>string</option><option value="int32">int32</option><option value="int64">int64</option><option value="float32">float32</option>';
		   	text += '<option value="float64">float64</option><option value="binary" disabled="disabled">binary</option>';
		   	text += '</select></div>';
		   	text += '<div class="r_box" style="width:60px;"><input name="indexHbaseColumnMaxLength" id="indexHbaseColumnMaxLength' + columnId + '" type="text" class="textbox1" style="width:60px;height:25px;" placeholder="最大长度"></div>&nbsp;&nbsp;';
		   	text += '<div class="r_box" style="width:10px;padding-top:3px;text-align:right;"><a href="#"><span class="glyphicon glyphicon-plus" title="增加列" onclick="addIndexCreateHbaseColumn();"></span></a></div>';
		   	 
    		text += '<div class="r_box" style="width:10px;padding-top:3px;"><a href="#"><span class="glyphicon glyphicon-minus" title="删除列" onclick="removeIndexCreateHbaseColumn(' + columnId + ');"></span></a></div>';
    		text += '</div>';
    		$("#indexHbaseLastColumn").before(text);
    		columnId++;
    		$("input[type='text']").focus(function() {
    			$("#indexCreateStatus").html("");
        	});
    	}
    	
    	function removeIndexCreateHbaseColumn(id) {
    		var removeId = "#indexCreateHbaseColumn" + id;
    		$(removeId).remove();
    	}
    	
    	/* function indexPatternCheck(operation, pattern){
 			if (operation == 0){
 	    		if (pattern == 1){
 	    			$("#endKeyIndexCreate").attr("disabled", true);
 	    		}
 	    		else {
 	    			$("#endKeyIndexCreate").attr("disabled", false);
 	    		}
 			}
    	} */
    	
    	function indexCreateTypeSelect() {
    		$("#indexCreateStatus").html("");
    		if ($("#indexCreateType").val() == 1) {
 				document.getElementById("indexCreateMode").style.display = "none";
 				document.getElementById("indexCreateSolrColumns").style.display = "none";
 				document.getElementById("indexCreateHbaseColumns").style.display = "block";
 			}
 			else if ($("#indexCreateType").val() == 0) {
 				document.getElementById("indexCreateMode").style.display = "block";
 				document.getElementById("indexCreateSolrColumns").style.display = "block";
 				document.getElementById("indexCreateHbaseColumns").style.display = "none";
 			}
    	}
    	
    	function indexHbaseColumnTypeSelect(id) {
    		$("#indexCreateStatus").html("");
    		var columnTypeId = "#indexHbaseColumnType" + id;
    		var columnMaxLenId = "indexHbaseColumnMaxLength" + id;
    		if ($(columnTypeId).val() == "string") {
    			document.getElementById(columnMaxLenId).style.display = "block";
    		}
    		else {
    			document.getElementById(columnMaxLenId).style.display = "none";
    		}
    		
    	}
 		
 		function indexCreate(){
 			var indexCreateMap = {};
 			$("#indexCreateStatus").html("");
     		if(!$("#indexNameCreate").val()){
     			document.getElementById("indexNameCreate").focus();
     			$("#indexCreateStatus").html("");
     			$("#indexCreateStatus").append("请输入索引名。");
     			return;
     		} 		
     		
    		//索引名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
    		if(!isValidName($("#indexNameCreate").val()))
    		{
    			document.getElementById("indexNameCreate").focus();
    			$("#indexCreateStatus").html("");
        		$("#indexCreateStatus").append("创建索引任务提交错误: " + "索引名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
    			return;
    		}
    		
    		if($("#indexNameCreate").val().length > 64)
    		{
    			document.getElementById("indexNameCreate").focus();
    			$("#indexCreateStatus").html("");
        		$("#indexCreateStatus").append("索引名长度不可以超过64个字符");
    			return;
    		}

     		var indexColumnInfos = [];
     		var indexColumns = [];
     		var indexMaxLens = [];
     		var k = 0;
     		var indexMaxLenNum = 0;
    		var rex_check_str2 = /^[1-9]\d*$/;		//if positive integer
    		

			indexCreateMap["type"] = $("#indexCreateType").val();
    		if ($("#indexCreateType").val() == 0) {
        		if(!$("#indexShardCreate").val()){
        			document.getElementById("indexShardCreate").focus();
         			$("#indexCreateStatus").html("");
         			$("#indexCreateStatus").append("请输入分片数。");
         			return;
         		} 	
        		if(!rex_check_str2.test($("#indexShardCreate").val()))
        		{
        			document.getElementById("indexShardCreate").focus();
        			$("#indexCreateStatus").html("");
            		$("#indexCreateStatus").append("分片数只能为正整数。");
        			return;
        		}
        		indexCreateMap["shard_num"] = $("#indexShardCreate").val();
        		indexCreateMap["pattern"] = $("input[name='indexPatternCreate']:checked").val();

         		$("#indexCreateSolrColumns").children(".tablebg2").each(function() {
         			if ($(this).find("#indexSolrColumnName").val()){
         				indexColumnInfos[k] = {"column":$(this).find("#indexSolrColumnName").val(),"type":$(this).find("#indexSolrColumnType").val()};
         				indexColumns[k] = $(this).find("#indexSolrColumnName").val();
         				k++;
         			}
         		});
    		}
    		else if ($("#indexCreateType").val() == 1) {
    			$("#indexCreateHbaseColumns").children(".tablebg2").each(function() {
         			if ($(this).find("[name='indexHbaseColumnName']").val()){
         				indexColumnInfos[k] = {};
         				indexColumnInfos[k]["column"] = $(this).find("[name='indexHbaseColumnName']").val();
         				indexColumnInfos[k]["type"] = $(this).find("[name='indexHbaseColumnType']").val();
         				if (indexColumnInfos[k]["type"] == "string") {
         					indexMaxLenNum++;
         					if ($(this).find("[name='indexHbaseColumnMaxLength']").val()) {
             					indexColumnInfos[k]["maxLen"] = $(this).find("[name='indexHbaseColumnMaxLength']").val();
             					indexMaxLens.push(indexColumnInfos[k]["maxLen"]); 	
         					}
         				}
         				indexColumns[k] = $(this).find("[name='indexHbaseColumnName']").val();
         				k++;
         			}
         		});    			
    		}
    		/* if (args_pktype == 1) {
    			if ($("#hashKeyIndexCreate").val()) {
        			indexCreateMap["hash_key"] = $("#hashKeyIndexCreate").val();
        		}
    			if ($("#startKeyIndexCreate").val()){
    				indexCreateMap["start_key"] = $("#startKeyIndexCreate").val();
    			}
    			
    			if ($("#endKeyIndexCreate").val()){
    				indexCreateMap["end_key"] = $("#endKeyIndexCreate").val();
    			}
    		} */
    		
			
     		//if ($("in<put[name='indexPatternCreate']:checked").val() == 0 && endKey == null){
     		//	$("#indexCreateStatus").html("");
     		//	$("#indexCreateStatus").append("离线索引必须提供终止键。");
			//	return;
			//}
     		
    		clearStatusHtml();
     		if (indexColumnInfos.length == 0){
     			$("#indexCreateStatus").append("请输入列名。");
     			return;
     		}
     		if (isRepeat(indexColumns)) {
     			$("#indexCreateStatus").append("列名不可重复。");
     			return;
     		}
     		if ($("#indexCreateType").val() == 1) {
     			var flag = true;
     			if (indexMaxLens.length < indexMaxLenNum) {
            		$("#indexCreateStatus").append("基于Hbase索引string类型的列必须输入“最大长度”。");
        			return;
     			}
     			for (var i=0; i<indexMaxLens.length; i++) {
     				if(!rex_check_str2.test(indexMaxLens[i])) {
     					flag = false;
     					break;
            		}
     			}
     			if (!flag) {
            		$("#indexCreateStatus").append("“最大长度”只能为正整数。");
        			return;
     			}
     		}
     		indexCreateMap["columns"] = indexColumnInfos;
     		
				$.ajax({
	                type: "POST",
	                url: "/otscfgsvr/api/index/" + modal_table_name + "/" + $("#indexNameCreate").val(),
	                data: JSON.stringify(indexCreateMap),
	                dataType: "json",
	                contentType: "application/json",
	                timeout: 30000,
	                success: function (results, msg) {
	                	$("#indexCreateStatus").append("创建索引任务提交").append(errorInfo(results["errcode"]));
	                	indexInit();	                	
	                },
	            	error: function(msg){
    	             	var errmsg = "创建索引任务提交失败！错误: " + getStructMsg(msg);
    	               	$("#indexCreateStatus").append(errmsg);
	                }
	            });
     	}
 		
 		function clearStatusHtml() {
 			$("#indexCreateStatus").html("");
 		}
    </script>

