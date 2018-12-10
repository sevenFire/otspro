<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
    <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">策略删除</h4>
</div>
<div class="modal-body">
    <div class="row" style="margin-top:20px;margin-left:15px;margin-right:15px;">
    	<input name="deleteType" id="timestampDeleteRadio" type="radio" class="" value="0" checked>按时间：
	    <input name="" id="records_starttime" type="text" class="Wdate" value="" style="height:26px;width:165px;" placeholder="起始时间" onclick="textFocusRecordDelete(0,-1);">&nbsp;&nbsp;
	    <input name="" id="records_endtime" type="text" class="Wdate" value="" style="height:26px;width:165px;" placeholder="终止时间（必填）" onclick="textFocusRecordDelete(0,-1);">
	</div>
	<div class="row" style="margin-left:12px;margin-right:15px;">
	    <div class="r_box" style="width:80px;"><input name="deleteType" id="primaryKeyDeleteRadio" type="radio" class="" value="1">按主键：</div>
	</div>
	<div class="row" style="margin-left:15px;margin-right:15px;">
	    <div class="text14" style="width:75px;text-align:right;">Hash键：&nbsp;</div>
	    <div class="col-md-5" style="padding:0px;"><input name="" id="hashKeyDelete" type="text" class="" value="" style="width:165px;" placeholder="（必填）"  onFocus="textFocusRecordDelete(1,-1);"></div>
	</div>
	<div class="row" id="recordDeleteRangeKey" style="margin-left:15px;margin-right:15px;display:none;">
		<div class="row" style="margin-top:0px;">
	      	<div class="text14" style="width:75px;text-align:right;">Range键：&nbsp;</div>
		    <div class="col-md-2" style="padding:0px;"><input name="rangeKeyDeleteType" id="recordDeleteRangePrefixKeyRadio" type="radio" class="" value="0" checked>前缀：</div>
		    <div class="col-md-5" style="padding:0px;"><input name="" id="recordDeleteRangePrefixKey" type="text" class="" value="" placeholder="前缀 " onclick="textFocusRecordDelete(1,0);"></div>
	     </div>
	     <div class="row" style="margin-top:5px;">
	      	<div class="text14" style="width:75px;">&nbsp;</div>
	      	<div class="col-md-2" style="padding:0px;"><input name="rangeKeyDeleteType" id="recordDeleteRangeStartEndKeyRadio" type="radio" class="" value="1">范围：</div>
	      	<div class="col-md-5" style="padding:0px;"><input name="" id="recordDeleteRangeStartKey" type="text" class="" value="" placeholder="起始键 " onclick="textFocusRecordDelete(1,1);"></div>
	     </div>
	     <div class="row" style="margin-top:5px;">
	     	<div class="text14" style="width:75px;">&nbsp;</div>
	     	<div class="col-md-2" style="padding:0px;">&nbsp;</div>
	      	<div class="col-md-5" style="padding:0px;"><input name="" id="recordDeleteRangeEndKey" type="text" class="" value="" placeholder="终止键 " onclick="textFocusRecordDelete(1,1);"></div>
	     </div>
	</div>
    <div class="row" style="height:20px;margin-top:5px;"><div class="r_text14" id="strategyDeleteStatus" style="width:450px;text-align:center"></div></div>
    <div class="row" style="width:450px;margin-top:0px;">
    	<div class="cy_text">
    		<input id="btn_strategyDelete" name="" type="button" class="btn2" style="width:60px" value="删除" onclick='strategyDelete();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    		<input id="btn_cancelStrategyDelete" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
    	</div>
    </div>
</div>

<script type="text/javascript">
	$(function(){
		if (args_pktype == 1) {
			document.getElementById("recordDeleteRangeKey").style.display = "block";
			if (args_range != null && args_range != 0) {
   				$("#recordDeleteRangePrefixKey").attr("disabled","disabled");
   			}
		}
	});
	
	function strategyDelete(){
    	$("#strategyDeleteStatus").html("");
    	var errmsg = "";
    	var key_type = $("input[name='deleteType']:checked").val();
    	var url = "/otscfgsvr/api/record/" + modal_table_name + "?";
 		
 		if (key_type == 0){
 			var start_time = $("#records_starttime").val();
 			if (start_time) {
 				url += "start_time=" + start_time + ":0&";
 			}
 			var end_time = $("#records_endtime").val();
 			if (!end_time) {
 				errmsg = "请输入终止时间。";
 				$("#strategyDeleteStatus").append(errmsg);
 				return;
 			} 
 			url += "end_time=" + end_time + ":0";
			$.ajax({
				type: "DELETE",
	 			url: url,
	 			timeout:30000,
	            success:function(results, msg){
	            	$("#strategyDeleteStatus").append("删除记录").append(errorInfo(results["errcode"]));
	            	getRecordList();
				},
				error: function(msg){
	            	errmsg = "删除记录失败！错误: " + getStructMsg(msg);
	            	$("#strategyDeleteStatus").append(errmsg);
	            }
			});
 		}
 		else if (key_type == 1){
 			if (!$("#hashKeyDelete").val()) {
 				errmsg = "请输入Hash键。";
 				$("#strategyDeleteStatus").append(errmsg);
 				return;
 			} 	    
 			var hashkey = $("#hashKeyDelete").val();
 			url += "hash_key=" + encodeURIComponent(hashkey);
 			if (args_pktype == 1) {
 				var range_type = $("input[name='rangeKeyDeleteType']:checked").val();
 				if (range_type == 0 && $("#recordDeleteRangePrefixKey").val()) {
 					url += "&range_key_prefix=" + encodeURIComponent($("#recordDeleteRangePrefixKey").val());
 				}
 				else if (range_type == 1) {
 	 	 			if ($("#recordDeleteRangeStartKey").val()) {
 	 	 				url += "&range_key_start=" + encodeURIComponent($("#recordDeleteRangeStartKey").val());
 	 	 			}
 	 	 			if ($("#recordDeleteRangeEndKey").val()) {
 	 	 				url += "&range_key_end=" + encodeURIComponent($("#recordDeleteRangeEndKey").val());
 	 	 			}
 				}
 			}
			$.ajax({
				type: "DELETE",
	 			url: url,
	 			timeout:30000,
	            success:function(results, msg){
	            	$("#strategyDeleteStatus").append("删除记录").append(errorInfo(results["errcode"]));
	            	getRecordList();
				},
				error: function(msg){
	            	errmsg = "删除记录失败！错误: " + getStructMsg(msg);
	            	$("#strategyDeleteStatus").append(errmsg);
	            }
			});
 		}
	}

	function textFocusRecordDelete(deleteType, rangeType){
	    	if (deleteType == 0) {
	    		$("#timestampDeleteRadio").attr("checked", true);
	    		datePickerInit();
	    	}
	    	else if (deleteType == 1) {
	    		$("#primaryKeyDeleteRadio").attr("checked", true);
	    		if (rangeType == 0) {
	    			$("#recordDeleteRangePrefixKeyRadio").attr("checked", true);
	    		}
	    		else if (rangeType == 1) {
	    			$("#recordDeleteRangeStartEndKeyRadio").attr("checked", true);
	    		}
	    	}
	    	$("#strategyDeleteStatus").html("");
	 	}
	
	function datePickerInit() {
		WdatePicker({
			dateFmt:'yyyy-MM-dd HH:mm:ss',
			readOnly:true
		});
	}
</script>
