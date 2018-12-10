<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 

<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表的恢复</h4>
</div>
<div class="modal-body">
	<form class="" id="restoreForm" method="get" action="">
   		<div  class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">连接类型： </div>
    			<div class="r_box">
    				<select id="tr_conn" style="background-color:#EBEBE4;" disabled="disabled">
     					<option value="0" selected>FTP</option>
     				</select>
     			</div>     
   		</div>
		<div  class="row">
			<div class="r_text" style="width:100px;padding-top:3px;">*URL：</div>
     		<div class="r_box" style="width:200px;"><input name='tr_host' id='tr_host' type='text' class='textbox1' style='width:200px;height:25px;'></div>
     	</div>
   		<div class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">*端口：</div>
     		<div class="r_box" style="width:200px;"><input name="tr_port" id="tr_port" type="text" class="textbox1" style="width:50px;height:25px;" value="21"></div>
    	</div>
		<div  class="row">
			<div class="r_text" style="width:100px;padding-top:3px;">用户名：</div>
     		<div class="r_box" style="width:200px;"><input name='tr_user' id='tr_user' type='text' class='textbox1' style='width:200px;height:25px;'></div>
     	</div>
   		<div class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">密码：</div>
     		<div class="r_box" style="width:200px;"><input name="tr_password" id="tr_password" type="password" class="textbox1" style="width:200px;height:25px;"></div>
    	</div>
     	<div class="row">	     				
	    	<div class="r_text" style="width:100px;padding-top:3px;">*目录：</div>
     		<div class="r_box" style="width:200px;"><input name="tr_directory" id="tr_directory" type="text" class="textbox1" style="width:200px;height:25px;"></div>
		</div> 
       	<div class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">*恢复表名：</div>
     		<div class="r_box" style="width:200px;"><input name="tr_tablenameNew" id="tr_tablenameNew" type="text" class="textbox1" style="width:200px;height:25px;"></div>
     		<div class="r_box" style="width:10px;margin-left:0px;padding-top:3px;"><span class="glyphicon glyphicon-info-sign" title="该表必需已存在。恢复数据无法覆盖已有数据。"></span></div>
    	</div>
    	<div class="row" style="margin-left:10px;margin-top:0px;height:20px;"><div class="r_text14" id="tableRestoreStatus" style="margin-left:10px;width:350px;text-align:center"></div></div>
    	<div  class="row" style="margin-top:0px;">
     		<div class="r_text" style="width:100px;padding-top:5px;"></div>
     		<div class="r_box">
     			<input name="tr_certain" type="submit" class="btn2" style="width:60px" value="恢复">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
     			<input name="tr_cancel" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
     		</div>
   		</div>
   		<div  class="row"></div>
	</form> 
</div>

 <script type="text/javascript">
	//var tablename = modal_table_name;
	var all_tables = all_table_info;
 	$(function(){ 
		$("#tableRestoreStatus").html("");
		//$("#tr_tablename").attr("value", tablename);
		//$("#tr_tablenameNew").attr("value", tablename);

		$.validator.setDefaults({
		    submitHandler: function() {
		    	tableRestore();
		    }
		});
		$("#restoreForm").validate({
			debug: true,
			rules: {
				tr_host: {
					required: true
				},
				tr_port: {
					required: true,
					digits: true
				},
				tr_directory: {
					required: true
				},
				tr_tablenameNew: {
					required: true,
					maxlength: 128
				}
			},
			errorClass: "r_text14",
			errorElement: "span",
		});
 	});

 	function tableRestore(){
	 	$("#tableRestoreStatus").html("");
	 	var conn = $("#tr_conn").val();
	 	var host = $("#tr_host").val();
	 	var port = $("#tr_port").val();
	 	var user = $("#tr_user").val();
	 	var password = $("#tr_password").val();
	 	var directory = $("#tr_directory").val();
	 	var filename = $("#tr_tablenameNew").val();

		if (directory.indexOf("/") != 0) {
			$("#tableRestoreStatus").html("");
    		$("#tableRestoreStatus").append("目录只能为绝对目录（以“/”开头）。");
			return;
		}

		if(!isValidName(filename))
		{
			$("#tableRestoreStatus").html("");
    		$("#tableRestoreStatus").append("表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
			return;
		}
		var table_num = 0;
		var table_index = -1;
		for (var i=0; i<all_tables.length; i++){
			if(all_tables[i]["table_name"] == filename){
				table_index = i;
				table_num++;
			}
		}
		if (table_num <= 0) {
			$("#tableRestoreStatus").html("");
    		$("#tableRestoreStatus").append("恢复表不存在，请先创建该表。");
			return;
		}
 		$.ajax({
            type: "POST",
            url: "/otscfgsvr/api/table/_restore",
            data: JSON.stringify({"host":host,"port":port,"username":user,"password":password,"mode":conn,"filename":filename,"dst":directory}),
            dataType: "json",
            contentType: "application/json",
            timeout: 30000,
            success: function (results, msg) {
            	$("#tableRestoreStatus").html("");
				$("#tableRestoreStatus").append("恢复表任务提交").append(errorInfo(results["errcode"]));
				if(results["errcode"] == 0){
					if (table_index > -1) {
						getOneTableBackupStatus(all_tables[table_index], table_index);
					}
				}				
				//getTableInfo();
            },
        	error: function(msg){
        		$("#tableRestoreStatus").html("");
            	var errmsg = "恢复表任务提交错误: " + getStructMsg(msg);
            	if (msg["status"] == 400 && msg["responseText"]) {
                	var pos = msg["responseText"].indexOf("\"errcode\":");
                	if (pos > 0) {
                		errmsg += "<br/>ErrorCode: " + msg["responseText"].substring(pos+10, pos+16);
                	}
            	}
            	$("#tableRestoreStatus").append(errmsg);
            }
        });
	}
 </script>
