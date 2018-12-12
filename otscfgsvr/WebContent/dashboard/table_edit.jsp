<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%>
<div class="modal-header" style="background-color:#f5f5f5;">
	<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
	<h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表的编辑</h4>
</div>

<div class="modal-body">
	<%--<div class="steps-container">--%>
	<%--<ul class="wizard-steps">--%>
	<%--<li id="wizard-step1" class="active">--%>
	<%--<span class="step">1</span>--%>
	<%--<span class="title">基本信息</span>--%>
	<%--</li>--%>
	<%--<li id="wizard-step2">--%>
	<%--<span class="step">2</span>--%>
	<%--<span class="title">列信息</span>--%>
	<%--</li>--%>
	<%--<li id="wizard-step3">--%>
	<%--<span class="step">3</span>--%>
	<%--<span class="title">主键配置</span>--%>
	<%--</li>--%>
	<%--</ul>--%>
	<%--</div>--%>
	<%--<div id="myCarousel" class="carousel slide" data-wrap="false">--%>
	<%--<div class="carousel-inner">--%>
	<%--<div class="item active">--%>
	<div style="height:360px;width:850px;">
		<div class="row" style="height:40px;margin-left:130px;"></div>
		<div class="row" id="rowTableInfoTc" style="margin-left:70px;height:180px;">
			<div  class="row" style="margin-top:0px;">
				<div class="r_text" style="width:100px;padding-top:3px;">*表名：</div>
				<div class="r_box" style='width:370px;'>
					<input name='tc_tableName' id='tc_tableName' type='text' class='' style='width:350px;height:25px;' value='' disabled="disabled">
					<span class="glyphicon glyphicon-exclamation-sign" title="注意：一旦创建表名不可更改！"></span>
				</div>
				<div class="r_text14" id="tc_noTableName" style="padding-top:10px;width:45px;display:none;">必填！</div>
			</div>

			<div  class="row" style="margin-top:0px;">
				<div class="r_text" style="width:100px;padding-top:3px;">描述信息：</div>
				<div class="r_box">
					<textarea id="tc_description" class="textbox1" style="width:350px;height:80px;max-height:160px;resize:none;" onKeypress="checkEnter(event)" ></textarea>
				</div>
			</div>

		</div>
		<div class="row" style="margin-left:50px;margin-top:60px;height:40px;">
			<div class="r_text14" id="tc_infoStatus" data-name="tc_status" style="margin-left:10px;width:550px;text-align:center"></div>
		</div>
		<div  class="row" style="margin-top:0px;margin-left:0px;">
			<div class="r_box" style="width:100%;text-align:center">
				<input id="tc_cancel1" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
				<input id="btn_edittable" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='tableUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			</div>
		</div>
	</div>
	<%--</div>--%>

	<%--<div class="item">--%>
	<%--<div style="height:360px;width:850px;">--%>
	<%--<div class="row" id="" style="width:800px;overflow-y:auto;margin-left:10px;margin-top:0;">--%>
	<%--<div class="r_box" style="width:130px;"><div class="c_text" style="width:130px;padding-top:3px;">*列名</div></div>--%>
	<%--<div class="r_box" style="width:80px;"><div class="c_text" style="width:80px;padding-top:3px;">数据类型</div></div>--%>
	<%--<div class="r_box" style="width:80px;"><div class="c_text" style="width:80px;padding-top:3px;">描述</div></div>--%>
	<%--</div>--%>
	<%--<div class="row" id="tc_tableColumns" style="overflow-y:auto;height:256px;margin-top:0;">--%>
	<%--<div class="tablebg2" id="tc_tableColumn1" style="width:650px;height:40px;margin-left:10px;margin-bottom:5px;">--%>
	<%--<div class="r_box" style="width:130px;">--%>
	<%--<input name="tc_tableColumnName1" id="tc_tableColumnName1" type="text" class="textbox1" style="width:130px;height:25px;">--%>
	<%--</div>--%>
	<%--<div class="r_box" style="width:80px;">--%>
	<%--<select name="tc_tableColumnType1" id="tc_tableColumnType1" style="width:80px;height:25px;" onchange="onSelectColumnType(1)">--%>
	<%--<option value="0">int8</option>--%>
	<%--<option value="1">int16</option>--%>
	<%--<option value="2">int32</option>--%>
	<%--<option value="3">int64</option>--%>
	<%--<option value="4">float</option>--%>
	<%--<option value="5">double</option>--%>
	<%--<option value="6">bool</option>--%>
	<%--<option value="7">string</option>--%>
	<%--</select>--%>
	<%--</div>--%>
	<%--<div class="r_box" style="width:80px;">--%>
	<%--<input name="tc_tableColumnDesc1" id="tc_tableColumnDesc1" type="text" class="textbox1" style="width:80px;height:25px;">--%>
	<%--</div>--%>
	<%--<div class="r_box" style="width:40px;padding-top:3px;text-align:left;">--%>
	<%--<span class="glyphicon glyphicon-plus" title="增加列" onclick='addTableColumn();'></span>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--<div class="row" id="tc_lastTableColumn" style="display:none"></div>--%>
	<%--</div>--%>


	<%--<div class="row" style="margin-left:50px;margin-top:0px;height:40px;">--%>
	<%--<div class="r_text14" id="tc_columnStatus" data-name="tc_status" style="margin-left:10px;width:550px;text-align:center"></div>--%>
	<%--</div>--%>
	<%--<div  class="row" style="margin-top:0px;margin-left:0px;">--%>
	<%--<div class="r_box" style="width:100%;text-align:center">--%>
	<%--<input id="tc_back2" name="" type="button" class="btn2" style="width:80px" value="<<返回" href="#myCarousel" onclick='goBack(2);' data-slide="prev">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
	<%--<input id="tc_cancel2" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
	<%--<input id="btn_edittable2" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='tableUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
	<%--<input id="tc_forward2" name="" type="button" class="btn2" style="width:80px;" value="继续>>" href="#myCarousel" onclick="configKeyColumn()" >--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>

	<%--<div class="item">--%>
	<%--<div  style="overflow-y:auto;height:360px;width:850px;">--%>

	<%--<div  id="configKeyDiv" style="overflow-y:auto;height:231px;width:51%;float: left;margin-right: 10px">--%>
	<%--<table id="configKeyTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
	<%--<thead>--%>
	<%--<tr>--%>
	<%--<th data-field="name" data-align="left"   >列名</th>--%>
	<%--<th data-field="type" data-align="left"   data-formatter="KeySelectFormatter" data-events="operateEvents">*键类型</th>--%>
	<%--</tr>--%>
	<%--</thead>--%>
	<%--</table>--%>
	<%--</div>--%>

	<%--<div  id="HashDiv" style="overflow-y:auto;height:231px;width:22%;float: left;margin-right: 10px">--%>
	<%--<table id="HashTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
	<%--<thead>--%>
	<%--<tr>--%>
	<%--<th data-field="colName_hash" data-align="left">Hash键列</th>--%>
	<%--<th data-field="operate_hash" data-align="left" data-formatter="HashKeyOperateFormatter" data-events="operateEvents">操作</th>--%>
	<%--</tr>--%>
	<%--</thead>--%>
	<%--</table>--%>
	<%--</div>--%>

	<%--<div  id="RangeDiv" style="overflow-y:auto;height:231px;width:22%;float: left;">--%>
	<%--<table id="RangeTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
	<%--<thead>--%>
	<%--<tr>--%>
	<%--<th data-field="colName_range" data-align="left">Range键列</th>--%>
	<%--<th data-field="operate_hash" data-align="left" data-formatter="RangeKeyOperateFormatter" data-events="operateEvents">操作</th>--%>
	<%--</tr>--%>
	<%--</thead>--%>
	<%--</table>--%>
	<%--</div>--%>

	<%--<div class="row" style="margin-top:0px;margin-left:0px;">--%>
	<%--<div class="r_box" style="width:100%;text-align:center">--%>
	<%--<input id="tc_back3" name="" type="button" class="btn2" style="width:80px" value="<<返回" href="#myCarousel" onclick='goBack(3);' data-slide="prev">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
	<%--<input id="btn_edittable3" name="" type="button" class="btn2" style="width:60px" value="更新" onclick='tableUpdate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
	<%--<input id="tc_cancel3" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>
	<%--</div>--%>


	<script type="text/javascript">
        // 		var thresholdvalue = 0;
        $(function(){
//	 	    debugger;
            tableUpdateWindow(modal_table_name);
        });

        function tableUpdateWindow(tablename) {
            $("#tableUpdateStatus").html("");
            $("#tc_tableName").attr("value", tablename);

            //todo 描述信息录入
            $("#tc_description").attr("value", "tc_description");

//          $("#tc_description").attr("value", "");
//			for (var i=0; i<all_table_info.length; i++){
//				if(all_table_info[i]["table_name"] == tablename){
//
//	    			$("#tc_description").attr("value", all_table_info[i]["description"]);
//
//				}
//			}
        }

        function tableUpdate(){
            $("#tableUpdateStatus").html("");

            if($("#description2").val().length > 256)
            {
                $("#tableUpdateStatus").html("");
                $("#tableUpdateStatus").append("表描述长度不可以超过256个字符。");
                return;
            }

            //var desc = htmlEscape($("#description2").val());
            var desc = $("#tc_description").val();
            alert("更新表");
            return;
//	 		$.ajax({
//	            type: "PUT",
//	            url: "/otscfgsvr/api/table/" + $("#tc_tableName").val(),
//	            data: JSON.stringify({"description": desc}),
//	            dataType: "json",
//	            contentType: "application/json",
//	            timeout: 10000,
//	            success: function (results, msg) {
//	            	$("#tableUpdateStatus").append("更新表").append(errorInfo(results["errcode"]));
//	            	if(results["errcode"] ==0){
//	            		var $table = $("#tableManage").bootstrapTable();
//	    	            $table.bootstrapTable('updateRow', {
//	    	                index: table_row_index,
//	    	                row: {
//	    	                    description: desc
//	    	                }
//	    	           });
//	    	           getTableInfo();
//	            	}
//	            },
//	        	error: function(msg){
//                	var errmsg = "更新表错误: " + getStructMsg(msg);
//                	$("#tableUpdateStatus").append(errmsg);
//	            }
//	        });
        }
	</script>

