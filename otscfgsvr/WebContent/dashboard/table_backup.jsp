<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 

<div class="modal-header" style="background-color:#f5f5f5;">
    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
            <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表的备份</h4>
</div>
<div class="modal-body">
	<form class="" id="backupForm" method="get" action="">
	<div  class="row" style="margin-top:0px;">
			<div class="r_text" style="width:100px;padding-top:3px;">表名：</div>
     		<div class="r_box" style="width:200px;"><input name='' id='tb_tablename' type='text' class='textbox1' style='width:200px;height:25px;background-color:#EBEBE4;' disabled="disabled"></div>
     	</div>
   		<div  class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">连接类型： </div>
    			<div class="r_box">
    				<select id="tb_conn" style="background-color:#EBEBE4;" disabled="disabled">
     					<option value="0" selected>FTP</option>
     				</select>
     			</div>     
   		</div>
		<div  class="row">
			<div class="r_text" style="width:100px;padding-top:3px;">*URL：</div>
     		<div class="r_box" style="width:200px;"><input name="tb_host" id="tb_host" type="text" class='textbox1' style='width:200px;height:25px;'></div>
     	</div>
   		<div class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">*端口：</div>
     		<div class="r_box" style="width:200px;"><input name="tb_port" id="tb_port" type="text" class="textbox1" style="width:50px;height:25px;" value="21"></div>
    	</div>
		<div  class="row">
			<div class="r_text" style="width:100px;padding-top:3px;">用户名：</div>
     		<div class="r_box" style="width:200px;"><input name='tb_user' id='tb_user' type='text' class='textbox1' style='width:200px;height:25px;'></div>
     	</div>
   		<div class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">密码：</div>
     		<div class="r_box" style="width:200px;"><input name="tb_password" id="tb_password" type="password" class="textbox1" style="width:200px;height:25px;"></div>
    	</div>
     	<div class="row">	     				
	    	<div class="r_text" style="width:100px;padding-top:3px;">*目录：</div>
     		<div class="r_box" style="width:200px;"><input name="tb_directory" id="tb_directory" type="text" class="textbox1" style="width:200px;height:25px;"></div>
		</div> 
       	<div  class="row">
     		<div class="r_text" style="width:100px;padding-top:3px;">*备份表名：</div>
     		<div class="r_box"><input name="tb_filename" id="tb_filename" type="text" class="textbox1" style="width:200px;height:25px;"></div>
    	</div>
    	<div class="row" style="margin-left:10px;margin-top:0px;height:20px;"><div class="r_text14" id="tableBackupStatus" style="margin-left:10px;width:350px;text-align:center"></div></div>
    	<div  class="row" style="margin-top:0px;">
     		<div class="r_text" style="width:100px;padding-top:5px;"></div>
     		<div class="r_box">
     			<input name="tb_certain" type="submit" class="btn2" style="width:60px" value="备份">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
     			<input name="tb_cancel" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
     		</div>
   		</div>
   		<div  class="row"></div>
	</form> 
</div>

 <script type="text/javascript">
	var tablename = modal_table_name;
	var tableRow = modal_table_row;
	var rowIndex = table_row_index;
 	$(function(){ 
		$("#tableBackupStatus").html("");
		$("#tableBackupState").html("");
		$("#tb_tablename").attr("value", tablename);
		$("#tb_filename").attr("value", tablename);
		//getBackupStatus(tablename);

		$.validator.setDefaults({
		    submitHandler: function() {
		    	tableBackup();
		    }
		});
		$("#backupForm").validate({
			debug: true,
			rules: {
				tb_host: {
					required: true
				},
				tb_port: {
					required: true,
					digits: true
				},
				tb_directory: {
					required: true
				},
				tb_filename: {
					required: true,
					maxlength: 128
				}
			},
			errorClass: "r_text14",
			errorElement: "span",
		});
 	});
 	
 	

 	function tableBackup(){
	 	$("#tableBackupStatus").html("");
	 	var conn = $("#tb_conn").val();
	 	var host = $("#tb_host").val();
	 	var port = $("#tb_port").val();
	 	var user = $("#tb_user").val();
	 	var password = $("#tb_password").val();
	 	var directory = $("#tb_directory").val();
	 	var filename = $("#tb_filename").val();
	 		 	
		if(!isValidName($("#tb_filename").val()))
		{
			$("#tableBackupStatus").html("");
    		$("#tableBackupStatus").append("备份表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
			return;
		}
		if (directory.indexOf("/") != 0) {
			$("#tableBackupStatus").html("");
    		$("#tableBackupStatus").append("目录只能为绝对目录（以“/”开头）。");
			return;
		}
 		$.ajax({
            type: "POST",
            url: "/otscfgsvr/api/table/_backup/" + tablename,
            data: JSON.stringify({"host":host,"port":port,"username":user,"password":password,"mode":conn,"filename":filename,"dst":directory}),
            dataType: "json",
            contentType: "application/json",
            timeout: 30000,
            success: function (results, msg) {
            	$("#tableBackupStatus").html("");
				$("#tableBackupStatus").append("备份表任务提交").append(errorInfo(results["errcode"]));
				if(results["errcode"] == 0){
					getOneTableBackupStatus(tableRow, rowIndex);
				}				
            },
        	error: function(msg){
        		$("#tableBackupStatus").html("");
            	var errmsg = "备份表任务提交错误: " + getStructMsg(msg);
            	if (msg["status"] == 400 && msg["responseText"]) {
                	var pos = msg["responseText"].indexOf("\"errcode\":");
                	if (pos > 0) {
                		errmsg += "<br/>ErrorCode: " + msg["responseText"].substring(pos+10, pos+16);
                	}
            	}
            	$("#tableBackupStatus").append(errmsg);
            }
        });
	}
 </script>
