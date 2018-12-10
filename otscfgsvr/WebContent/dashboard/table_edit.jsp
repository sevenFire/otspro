<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
    <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表的编辑</h4>
</div>
<div class="modal-body">
	<div>
   		<div  class="row" style="margin-top:0px;">
     		<div class="r_text" style="width:110px;padding-top:3px;">*表名：</div>
     		<div class="r_box" style="width:165px;"><input name='tableNameText2' id='tableNameText2' type='text' class='' style='width:150px;height:25px;' value='' disabled="disabled"></div>
   		</div>
   		
	   <div  class="row">
     		<div class="r_text" style="width:110px;padding-top:3px;">*主键类型：</div>
    			<div class="r_box" style="width:165px;">
    				<select id="keyTypeTableUpdate" class="table-select-disabled" disabled="disabled">
     					<option value="0">Hash键</option>
     					<option value="1">Hash键 + Range键</option>
     				</select>
     			</div>
     	</div>
		<div class="row">
			<div class="r_text" style="width:110px;padding-top:3px;">*Hash键类型：</div>
			<div class="r_box" style="width:165px;">
    			<select id="hashKeyTypeTableUpdate" class="table-select-disabled" disabled="disabled">
     				<option value="0">STRING</option>
     				<option value="1">NUMBER</option>
     				<option value="2">BINARY</option>
     			</select>
     		</div>
     	</div>
     	<div class="row" id="rangeKeyTypeRowTableEdit" style="display:none;">
			<div class="r_text" style="width:110px;padding-top:3px;">Range键类型：</div>
			<div class="r_box" style="width:165px;">
    			<select id="rangeKeyTypeTableUpdate" class="table-select-disabled" disabled="disabled">
     				<option value="0">STRING</option>
     				<option value="1">NUMBER</option>
     				<option value="2">BINARY</option>
     			</select>
     		</div>
     	</div>
   		<div  class="row">
     		<div class="r_text" style="width:110px;padding-top:3px;">压缩算法： </div>
    		<div class="r_box" style="width:165px;">
    			<select id="compression2" class="table-select-disabled" disabled="disabled">
     					<option value="0">NONE</option>
     					<option value="1">SNAPPY</option>
     					<option value="2">LZ4</option>
     					<option value="3">GZ</option>
     					<!-- <option value="4">LZO</option> -->
     			</select>
     		</div>     
   		</div>
    	<!-- <div  class="row">
     		<div class="r_text" style="width:110px;padding-top:3px;">支持Mob：</div>
    			<div class="r_box" style="width:165px;">
    				<select id="enableMobSel2" class="table-select-disabled" disabled="disabled">
     					<option value="1">是</option>
     					<option value="0" selected>否</option>
     				</select>
     			</div>
     	</div>
     	<div id="threshold2" class="row" style="display:none;">	     				
	    	<div class="r_text" style="width:110px;padding-top:3px;">阈值：</div>
     		<div class="r_box" style="width:120px;"><input id="thresholdText2" type="text" class="" style="width:120px;height:25px;"></div>
    		<div class="r_text" style="width:30px;padding-top:3px;text-align:left">KB</div>
    		 
    		<div class="r_box" >
    			<select id="thresholdUnit1">
     				<option value="kb" selected>KB</option>
     				<option value="m" >M</option>
     			</select>
     		</div> 
     		
		</div>  -->
       	<div  class="row">
     		<div class="r_text" style="width:110px;padding-top:3px;">描述：</div>
     		<div class="r_box" style="width:165px;"><input name="" id="description2" type="text" class="textbox1" style="width:150px;height:25px;"></div>
    	</div>
        <div class="row" style="margin-top:0px;height:20px;"><div class="r_text14" id="tableUpdateStatus" style="width:350px;text-align:center"></div></div>
    	<div  class="row" style="margin-top:0px;">
     		<div class="r_text" style="width:90px;padding-top:5px;"></div>
     		<div class="r_box" style="width:240px;">
     			<input id="btn_edittable" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='tableUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
     			<input id="btn_canceledit" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
     		</div>
   		</div>
   		<div  class="row">
   		</div>
	</div>
</div>

 <script type="text/javascript">
 		var thresholdvalue = 0;
	 	$(function(){ 		
			tableUpdateWindow(modal_table_name);
	 	});
	 	
	 	function tableUpdateWindow(tablename) {
		 	$("#tableUpdateStatus").html("");
			$("#tableNameText2").attr("value", tablename);
			document.getElementById("compression2").options[0].selected = true;
			$("#description2").attr("value", "");
			for (var i=0; i<all_table_info.length; i++){
				if(all_table_info[i]["table_name"] == tablename){
					$("#keyTypeTableUpdate").attr("value", all_table_info[i]["primary_key_type"]);
					$("#hashKeyTypeTableUpdate").attr("value", all_table_info[i]["hash_key_type"]);
					if (all_table_info[i]["primary_key_type"] == 1) {
						document.getElementById("rangeKeyTypeRowTableEdit").style.display = "block";
						$("#rangeKeyTypeTableUpdate").attr("value", all_table_info[i]["range_key_type"]);
					}
	    			$("#compression2").attr("value", all_table_info[i]["compression_type"]);
	    			$("#description2").attr("value", all_table_info[i]["description"]);
	    			/* document.getElementById("enableMobSel2").options[0].selected = all_table_info[i]["mob_enabled"];
	    			$("#thresholdText2").attr("disabled",!all_table_info[i]["mob_enabled"]);
	    			if (all_table_info[i]["mob_enabled"] == 1) {
	    				document.getElementById("threshold2").style.display = "block";
	    				$("#thresholdText2").attr("value",all_table_info[i]["mob_threshold"]);
	    			} */
				}
			}
	 	}
	 	
	 	function tableUpdate(){	
	 		$("#tableUpdateStatus").html("");
		 	
			if($("#description2").val().length > 256)
			{
				$("#tableUpdateStatus").html("");
	    		$("#tableUpdateStatus").append("表描述长度不可以超过256个字符。");
				return;
			}
			
			/* if(enableMobSel2.value == 1) {			
				thresholdvalue = $("#thresholdText2").val();
				var value_test_str1 = /^\d+$/;
				if (!$("#thresholdText2").val() || !value_test_str1.test(thresholdvalue)
						|| parseInt(thresholdvalue, 10) < 100 || parseInt(thresholdvalue, 10) > 10240) {
					$("#tableUpdateStatus").html("");
		    		$("#tableUpdateStatus").append("阈值应为100-10240间的整数。");
		    		return;
				}
			} */
			//var desc = htmlEscape($("#description2").val());
			var desc = $("#description2").val();
	 		$.ajax({
	            type: "PUT",
	            url: "/otscfgsvr/api/table/" + $("#tableNameText2").val(),
	            data: JSON.stringify({"description": desc}),	            
	            dataType: "json",
	            contentType: "application/json",
	            timeout: 10000,
	            success: function (results, msg) {
	            	$("#tableUpdateStatus").append("更新表").append(errorInfo(results["errcode"]));
	            	if(results["errcode"] ==0){
	            		var $table = $("#tableManage").bootstrapTable();
	    	            $table.bootstrapTable('updateRow', {
	    	                index: table_row_index,
	    	                row: {
	    	                    description: desc
	    	                }
	    	           });	    	            
	    	           getTableInfo();
	            	}	            	
	            },
	        	error: function(msg){
                	var errmsg = "更新表错误: " + getStructMsg(msg);
                	$("#tableUpdateStatus").append(errmsg);
	            }
	        });
	    }
 </script>

