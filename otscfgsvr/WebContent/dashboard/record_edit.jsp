<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">编辑记录</h4>
</div>
<div class="modal-body">
	<div class="row" style="text-align:left;margin-top:0px;">记录详细信息：</div>
    <div class="row" style="height:35px;"><textarea name="" id="recordEditContent" cols="" rows="" style="width:550px;height:210px;max-width:550px;max-height:210px;resize:none;"></textarea></div>
    <div class="row" style="margin-top:180px;height:20px;"><div class="r_text14" id="recordUpdateStatus" style="margin-left:10px;width:550px;text-align:center"></div></div>
    <div class="row" style="width:550px;margin-top:0px;">   
    	<div class="cy_text">
    		<input id="btn_editrecord" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='recordUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    		<input id="btn_canceledit" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
    	</div>
    </div>
</div>

<script type="text/javascript">
	var keytype = args_pktype;
	var recordToEdit = {};
	var recordKeys = [];
	$(function(){
		$("#recordEditContent").attr("value", "");
		recordUpdateWindow();
	});
	
	 function recordUpdateWindow(){
		 var queryUrl = "query_from=0&hash_key=" + encodeURIComponent(modal_hash_key);
		 if (keytype == 1) {
			 queryUrl += "&range_key=" + encodeURIComponent(modal_range_key);
		 }
	    	$("#recordUpdateStatus").html("");	     		
	    	$.ajax({
	 			cache: false,
	    		type: "GET",
	    		url: "/otscfgsvr/api/record/" + modal_table_name + "?" + queryUrl,
	    		dataType: "json",
	    		timeout: 30000,
	    		success: function(results, msg){
	    			if (results["errcode"] == 0) {
	    				recordKeys = [];
		    	    	var recordContent = "{\n\"records\":\n\t[\n\t\t{\n";
		    			for (var i=0; i<results["records"].length; i++){
		    				if (modal_hash_key == results["records"][i]["hash_key"]){
		    					recordToEdit = results["records"][i];
		    					break;
		    				}
		    			}
		    			for(var item in recordToEdit){
		    				recordKeys.push(item);
		    			}
		    			for (var i=0; i<recordKeys.length; i++) {
		    				recordContent += "\t\t\"" + recordKeys[i] + "\":\"" + recordToEdit[recordKeys[i]] + "\"";
		    				if (i < recordKeys.length - 1){
			    				recordContent += ",\n";
		    				}
		    			}
		    	    	recordContent += "\n\t\t}\n\t]\n}";
		    	    	$("#recordEditContent").attr("value", recordContent);
	    			}
	    			else {
	    				$("#recordUpdateStatus").append("获取记录失败！错误：" + results["errcode"]);
	    			}
	    		},
	    		error: function (msg){
	            	var errmsg = "获取记录失败！错误: " + getStructMsg(msg);
	            	$("#recordUpdateStatus").append(errmsg);
	    		}
	    	});

	    }
		
		function recordUpdate(){
			$("#recordUpdateStatus").html("");			
			$.ajax({
				type:"PUT",
	 			url:"/otscfgsvr/api/record/" + modal_table_name,
	 			data:$("#recordEditContent").val(),
	 			dataType:"json",
	 			contentType: "application/json",
	 			timeout:30000,
	            success:function(results){   
	            	$("#recordUpdateStatus").append("更新记录").append(errorInfo(results["errcode"]));
	            	getRecordList();
	            },
	            error: function(msg){
	            	var errmsg = "更新记录失败！错误: " + getStructMsg(msg);
	            	$("#recordUpdateStatus").append(errmsg);
	            }
			});
		}

</script>