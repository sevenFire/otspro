<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%>

<div class="modal-header" style="background-color:#f5f5f5;">
	<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
	<h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表的创建-向导</h4>
</div>

<div class="modal-body">
	<div class="steps-container">
		<ul class="wizard-steps">
			<li id="wizard-step1" class="active">
				<span class="step">1</span>
				<span class="title">基本信息</span>
			</li>
			<li id="wizard-step2">
				<span class="step">2</span>
				<span class="title">列信息</span>
			</li>
			<%--<li id="wizard-step3">--%>
				<%--<span class="step">3</span>--%>
				<%--<span class="title">主键配置</span>--%>
			<%--</li>--%>
		</ul>
	</div>
	<div id="myCarousel" class="carousel slide" data-wrap="false">
		<div class="carousel-inner">
			<div class="item active">
				<div style="height:360px;width:1000px;">
					<div class="row" style="height:40px;margin-left:130px;"></div>
					<div class="row" id="rowTableInfoTc" style="margin-left:70px;height:180px;">
						<div  class="row" style="margin-top:0px;">
							<div class="r_text" style="width:100px;padding-top:3px;">*表名：</div>
							<div class="r_box" style='width:370px;'>
								<input id='tc_tableName' type='text' class='' style='width:350px;height:25px;' value=''>
								<span class="glyphicon glyphicon-exclamation-sign" title="注意：一旦创建表名不可更改！"></span>
							</div>
							<div class="r_text14" id="tc_noTableName" style="padding-top:10px;width:45px;display:none;">必填！</div>
						</div>

						<div  class="row" style="margin-top:0px;">
							<div class="r_text" style="width:100px;padding-top:3px;">描述信息：</div>
							<div class="r_box">
								<textarea id="tc_description" class="textbox1" style="width:350px;height:80px;max-height:160px;resize:none;" onKeypress="checkEnter(event)"></textarea>
							</div>
						</div>
					</div>
					<div class="row" style="margin-left:50px;margin-top:60px;height:40px;">
						<div class="r_text14" id="tc_infoStatus" data-name="tc_status" style="margin-left:10px;width:550px;text-align:center"></div>
					</div>
					<div  class="row" style="margin-top:0px;margin-left:0px;">
						<div class="r_box" style="width:100%;text-align:center">
							<input id="tc_cancel1" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
							<input id="tc_forward1" name="" type="button" class="btn2" style="width:80px" value="继续>>" href="#myCarousel" onclick='checkTableInfo();'>
						</div>
					</div>
				</div>
			</div>

			<div class="item">
				<div style="height:360px;width:1000px;">
					<div class="row" id="" style="width:900px;overflow-y:auto;margin-left:10px;margin-top:0;">
						<div class="r_box" style="width:130px;"><div class="c_text" style="width:130px;padding-top:3px;">*列名</div></div>
						<div class="r_box" style="width:80px;"><div class="c_text" style="width:80px;padding-top:3px;">数据类型</div></div>
						<div class="r_box" style="width:30px;"><div class="c_text" style="width:30px;padding-top:3px;">主键</div></div>
						<div class="r_box" style="width:30px;"><div class="c_text" style="width:30px;padding-top:3px;">排序</div></div>
					</div>
					<div class="row" id="tc_tableColumns" style="overflow-y:auto;height:256px;margin-top:0;">
						<div class="tablebg2" id="tc_tableColumn1" style="width:650px;height:40px;margin-left:10px;margin-bottom:5px;">
							<div class="r_box" style="width:130px;">
								<input name="tc_tableColumnName1" id="tc_tableColumnName1" type="text" class="textbox1" style="width:130px;height:25px;">
							</div>
							<div class="r_box" style="width:80px;">
								<select name="tc_tableColumnType1" id="tc_tableColumnType1" style="width:80px;height:25px;" onchange="onSelectColumnType(1)">
									<option value="0">int8</option>
									<option value="1">int16</option>
									<option value="2">int32</option>
									<option value="3">int64</option>
									<option value="4">float</option>
									<option value="5">double</option>
									<option value="6">bool</option>
									<option value="7">string</option>
								</select>
							</div>
							<div class="r_box" style="width:30px;text-align:center">
								<input name="tc_tableColumnPk1" id="tc_tableColumnPk1" type="checkbox" onclick="disableSeq(1)" title="注意：一旦创建主键不可更改！">
							</div>
							<div class="r_box" style="width:30px;">
								<input name="tc_tableColumnSeq1" id="tc_tableColumnSeq1" type="text" class="textbox1" style="width:30px;height:25px;" >
								<%--<input name="tc_tableColumnSeq1" id="tc_tableColumnSeq1" type="text" class="textbox1" style="width:30px;height:25px;" disabled="disabled">--%>
							</div>
							<%--<div class="r_box" style="width:80px;">--%>
								<%--<input name="tc_tableColumnDesc1" id="tc_tableColumnDesc1" type="text" class="textbox1" style="width:80px;height:25px;">--%>
							<%--</div>--%>
							<div class="r_box" style="width:40px;padding-top:3px;text-align:left;">
								<span class="glyphicon glyphicon-plus" title="增加列" onclick='addTableColumn() ;'></span>
							</div>
						</div>
						<div class="row" id="tc_lastTableColumn" style="display:none"></div>
					</div>


					<div class="row" style="margin-left:50px;margin-top:0px;height:40px;">
						<div class="r_text14" id="tc_columnStatus" data-name="tc_status" style="margin-left:10px;width:550px;text-align:center"></div>
					</div>

					<div  class="row" style="margin-top:0px;margin-left:0px;">
						<div class="r_box" style="width:100%;text-align:center">
							<input id="tc_back2" name="" type="button" class="btn2" style="width:80px" value="<<返回" href="#myCarousel" onclick='goBack(2);' data-slide="prev">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
							<%--<input id="tc_cancel2" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
							<%--<input id="tc_forward2" name="" type="button" class="btn2" style="width:80px;" value="继续>>" href="#myCarousel" onclick="configKeyColumn()" >--%>
							<input id="tc_create" name="" type="button" class="btn2" style="width:80px" value="创建"  onclick='tableCreate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
							<input id="tc_cancel2" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">
						</div>
					</div>
				</div>
			</div>

			<%--<div class="item">--%>
				<%--<div  style="overflow-y:auto;height:360px;width:1000px;">--%>

					<%--<div  id="configKeyDiv" style="overflow-y:auto;height:231px;width:30%;float: left;margin-right: 10px">--%>
						<%--<table id="configKeyTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
							<%--<thead>--%>
								<%--<tr>--%>
									<%--<th data-field="name" data-align="left"   >列名</th>--%>
									<%--<th data-field="type" data-align="left"   data-formatter="KeySelectFormatter" data-events="operateEvents">*键类型</th>--%>
								<%--</tr>--%>
							<%--</thead>--%>
						<%--</table>--%>
					<%--</div>--%>

					<%--<div  id="HashDiv" style="overflow-y:auto;height:231px;width:32%;float: left;margin-right: 10px">--%>
						<%--<table id="HashTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
							<%--<thead>--%>
								<%--<tr>--%>
									<%--<th data-field="colName_hash" data-align="left">Hash键列</th>--%>
									<%--<th data-field="operate_hash" data-align="left" data-formatter="HashKeyOperateFormatter" data-events="operateEvents">操作</th>--%>
								<%--</tr>--%>
							<%--</thead>--%>
						<%--</table>--%>
					<%--</div>--%>

					<%--<div  id="RangeDiv" style="overflow-y:auto;height:231px;width:32%;float: left;">--%>
						<%--<table id="RangeTable" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">--%>
							<%--<thead>--%>
								<%--<tr>--%>
									<%--<th data-field="colName_range" data-align="left">Range键列</th>--%>
									<%--<th data-field="operate_hash" data-align="left" data-formatter="RangeKeyOperateFormatter" data-events="operateEvents">操作</th>--%>
								<%--</tr>--%>
							<%--</thead>--%>
						<%--</table>--%>
					<%--</div>--%>
					<%--<div class="row" style="margin-left:50px;margin-top:60px;height:40px;">--%>
						<%--<div class="r_text14" id="hr_infoStatus" data-name="hr_status" style="margin-left:10px;width:550px;text-align:center;" ></div>--%>
					<%--</div>--%>
					<%--<div class="row" style="margin-top:0px;margin-left:0px;">--%>
						<%--<div class="r_box" style="width:100%;text-align:center">--%>
							<%--<input id="tc_back3" name="" type="button" class="btn2" style="width:80px" value="<<返回" href="#myCarousel" onclick='goBack(3);' data-slide="prev">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
							<%--<input id="tc_create" name="" type="button" class="btn2" style="width:80px" value="创建"  onclick='tableCreate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--%>
							<%--<input id="tc_cancel3" name="" type="button" class="btn2" style="width:80px" value="取消" data-dismiss="modal">--%>
						<%--</div>--%>
					<%--</div>--%>
				<%--</div>--%>
			<%--</div>--%>
		</div>
	</div>
</div>

<script type="text/javascript">
    var tc_columns=[];
    var tc_tablename = "";
    var tc_tabledesc = "";
    var tableAddColumnId = 2;
    var tc_field_delimiter = ",";		//default for hive table
    var hiveTableAddColumnId = 1;
    var templateHiveCol;
    var rex_check_str_1 = /^([_A-Z0-9a-z])+$/;


        $('.carousel').carousel({
            interval: false
        }).unbind("keydown");

        $("input[type='text']").focus(function() {
            $("#tc_noTableName").css("display", "none");
            clearTcStatus();
        });
        $("select").focus(function() {
            clearTcStatus();
        });

//        $("#hiveColForm").validate({
//            errorPlacement: function(error, element) {
//                error.text(element.attr("data-tips") + "：" + error.text());
//                $("#tc_columnStatus").html(error);
//            },
//        });


    function checkEnter(e)
    {
        var et=e||window.event;
        var keycode=et.charCode||et.keyCode;

        if ((keycode==13)|| (keycode == 92))
        {
            if(window.event)
                window.event.returnValue = false;
            else
                e.preventDefault();//for firefox
        }
    }

//    window.operateEvents = {
//        'click .keyHash': function (e, value, row, index) {
//            clickSelectHashKey(row);
//        },
//        'click .keyRange': function (e, value, row, index) {
//            clickSelectRangeKey(row);
//        },
//        'click .keyUp': function (e, value, row, index) {
//            clickUpKey(row);
//        },
//        'click .keyDown': function (e, value, row, index) {
//            clickDownKey(row);
//        },
//        'click .keyDeleteHash': function (e, value, row, index) {
//            clickDeleteHashKey(row);
//        },
//        'click .keyDeleteRange': function (e, value, row, index) {
//            clickDeleteRangeKey(row);
//        },
//
//    };

    //选择为Hash键还是Range键
//    function KeySelectFormatter(value, row, index) {
//        return [
//            '<input name="" type="button" class="keyHash" style="width:75px;" value="Hash键"  data-target="#HashTable">',
//            '&nbsp;&nbsp;&nbsp;&nbsp;',
//
//            '<input name="" type="button" class="keyRange" style="width:75px;" value="Range键"   data-target="#RangeTable">',
//        ].join('');
//    }

//	//选择为Hash键还是Range键
//	function KeySelectFormatter(value, row, index) {
//		return [
//			'<input type="radio" class="keyHash" value="1"  name="HashRangeRadio' + index + '" id="HashKeyRadio' + index + '"  />Hash键',
//			'&nbsp;&nbsp;&nbsp;',
//			'<input type="radio" class="keyRange" value="0"  name="HashRangeRadio' + index + '" id="RangeKeyRadio' + index + '" />Range键'
//		].join('');
//	}

//    //hash键的操作（上、下移动、删除）,目前只支持删除
//    function HashKeyOperateFormatter(value, row, index) {
//        return [
//            '<input name="" type="button" class="keyUp" style="width:40px;" value="上移"  >',
//            '&nbsp;&nbsp;',
//
//            '<input name="" type="button" class="keyDown" style="width:40px;" value="下移"  >',
//            '&nbsp;&nbsp;',
//            '<input name="" type="button" class="keyDeleteHash" style="width:40px;" value="删除" data-method="remove" data-target="#HashTable">',
//
//        ].join('');
//    }
//    //Range键的操作（上、下移动、删除）,目前只支持删除
//    function RangeKeyOperateFormatter(value, row, index) {
//        return [
//            '<input name="" type="button" class="keyUp" style="width:30px;" value="上移"  >',
//            '&nbsp;&nbsp;',
//
//            '<input name="" type="button" class="keyDown" style="width:30px;" value="下移"  >',
//            '&nbsp;&nbsp;',
//            '<input name="" type="button" class="keyDeleteRange" style="width:40px;" value="删除"  data-method="remove" data-target="#RangeTable">',
//
//        ].join('');
//    }

//    function clickSelectHashKey(editRow) {
////        debugger
//        var hash_table = [];
//        var range_table = [];
//        var colName = editRow.name;
//        var hashKey_data = {"colName_hash": colName};
//		var count = 0;
//
//        range_table = $("#RangeTable").bootstrapTable('getData');
//        for (var ii in range_table) {
//            if (range_table[ii].colName_range == colName) {
//                $("#RangeTable").bootstrapTable('remove', {
//                    field: 'colName_range',
//                    values: [colName]
//                });
//                break;
//            }
//        }
//        hash_table = $("#HashTable").bootstrapTable('getData');
//        for (var i in hash_table) {
//            if (hash_table[i].colName_hash == colName) {
//                $("#hr_infoStatus").html("");
//                $("#hr_infoStatus").append("列已存在，请不要重复选择。");
//                break;
//            }
//            else{
//                count ++;
//			}
//        }
//        if (hash_table.length==0 || (count-1) == hash_table.length){
//
//
//            $("#HashTable").bootstrapTable('append', hashKey_data);//_data----->新增的数据
//            $("#hr_infoStatus").html("选为Hash键列。");
//			return;
//		}
//
//		return;
//
//
//    }



//	function clickSelectRangeKey(editRow) {
////                debugger
////        var rangeKey_data = {"colName_range":editRow.name};
////        $("#RangeTable").bootstrapTable('append', rangeKey_data);//_data----->新增的数据
//        var hash_table = [];
//        var range_table = [];
//        var colName = editRow.name;
//        var rangeKey_data = {"colName_range":colName};
//        var count_r = 0;
//        hash_table = $("#HashTable").bootstrapTable('getData');
//        for (var ii in hash_table) {
//            if (hash_table[ii].colName_hash == colName) {
//                $("#HashTable").bootstrapTable('remove', {
//                    field: 'colName_hash',
//                    values: [colName]
//                });
//                break;
//            }
//
//        }
//        range_table = $("#RangeTable").bootstrapTable('getData');
//        for (var i in range_table) {
//            if (range_table[i].colName_range == colName) {
//                $("#hr_infoStatus").html("");
//                $("#hr_infoStatus").append("列已存在，请不要重复选择。");
//                break;
//            }
//            else{
//                count_r ++;
//            }
//        }
//        if (range_table.length==0 || (count_r-1) == range_table.length){
////            $("#hr_infoStatus").style.display == 'none';
//            $("#RangeTable").bootstrapTable('append', rangeKey_data);//_data----->新增的数据
//            $("#hr_infoStatus").html("选为Range键列。");
//            return;
//        }
//
//        return;
//	}

//    function swapNode(node1,node2){
//        //获取父结点
//        var _parent = node1.parentNode;
//        //获取两个结点的相对位置
//        var _t1 = node1.nextSibling;
//        var _t2 = node2.nextSibling;
//        //将node2插入到原来node1的位置
//        if(_t1)_parent.insertBefore(node2,_t1);
//        else _parent.appendChild(node2);
//        //将node1插入到原来node2的位置
//        if(_t2)_parent.insertBefore(node1,_t2);
//        else _parent.appendChild(node1);
//    }


//    function clickUpKey(editRow) {
////        debugger
//        var _row = editRow.parentNode.parentNode;
//        //如果不是第一行，则与上一行交换顺序
//        var _node = _row.previousSibling;
//        while(_node && _node.nodeType != 1){
//            _node = _node.previousSibling;
//        }
//        if(_node){
//            swapNode(_row,_node);
//        }
//
//    }

//    function clickDownKey(editRow) {
////        debugger
//        var _row = editRow.parentNode.parentNode;
//        //如果不是最后一行，则与下一行交换顺序
//        var _node = _row.nextSibling;
//        while(_node && _node.nodeType != 1){
//            _node = _node.nextSibling;
//        }
//        if(_node){
//            swapNode(_row,_node);
//        }
//
//    }

//    function clickDeleteHashKey(editRow) {
////        debugger
//        $("#HashTable").bootstrapTable('remove',{
//            field: 'colName_hash',
//            values: [editRow.colName_hash]
//		});
//
//    }

//    function clickDeleteRangeKey(editRow) {
//        $("#RangeTable").bootstrapTable('remove',{
//            field: 'colName_range',
//            values: [editRow.colName_range]
//		});
//
//    }

    function checkTableInfo() {
        $("#tc_infoStatus").html("");
        //表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
        if(!isValidName($("#tc_tableName").val()))
        {
            $("#tc_infoStatus").html("");
            $("#tc_infoStatus").append("表命名错误: " + "表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
            return;
        }

        if(!$("#tc_tableName").val() || $("#tc_tableName").val().trim().length == 0){
            $("#tc_noTableName").show();
            return;
        }
        else{
            $("#tc_noTableName").hide();
        }



        tc_tablename = $("#tc_tableName").val().trim();
        tc_tabledesc = $("#tc_description").val();
        if(tc_tablename.length > 64) {
            $("#tc_infoStatus").html("");
            $("#tc_infoStatus").append("表名长度不能超过64个字符。");
            return;
        }

//	    else if(!rex_check_str1.test(tc_tablename)) {
        else if(!rex_check_str_1.test(tc_tablename)) {
            $("#tc_infoStatus").html("表名只能包含拉丁字母或数字或下划线“_”。");
            return;
        }

        if(tc_tabledesc.length > 256) {
            $("#tc_infoStatus").html("描述信息长度不能超过256个字符。");
            return;
        }

        $("#myCarousel").carousel('next');
        $("#wizard-step1").attr("class", "complete");
        $("#wizard-step2").attr("class", "active");
    }

    function initEncodingSelects(i) {
        initEncodingOption($("#tc_tableColumnType" + i).find("option:selected").text(), i, 0);
    }

    function onSelectColumnType(i) {
        initEncodingSelects(i);

        if ($('#tc_tableColumnType' + i).attr("value") == "4"
            || $('#tc_tableColumnType' + i).attr("value") == "5"
            || $('#tc_tableColumnType' + i).attr("value") == "6") {
            $('#tc_tableColumnPk' + i).css("display", "none");
            $('#tc_tableColumnSeq' + i).css("display", "none");

            $('#tc_tableColumnPk' + i).attr("checked", false);
            $('#tc_tableColumnSeq' + i).attr("value", "");
        } else {
            $('#tc_tableColumnPk' + i).css("display", "inline");
            $('#tc_tableColumnSeq' + i).css("display", "inline");
        }
    }

    function checkColumnNameCreate(name) {
        return checkColumnName(name, 0);
    }

    function checkColumnName(name, position) {		//position: 0-table_create; 1-column_add
        var statusId = "";
        switch (position) {
            case 0:
                statusId = "#tc_columnStatus";
                break;
            case 1:
                statusId = "#columnAddStatus";
                break;
            default:
                break;
        }
        if (name == "") {
            $(statusId).html("列名不能为空。");
            return false;
        }
        if(name.length > 64) {
            $(statusId).html("列名长度不能超过64个字符。");
            return false;
        }
//        else if(!rex_check_str1.test(name)) {
        else if(!rex_check_str_1.test(name)) {
            $(statusId).html("列名只能包含拉丁字母或数字或下划线“_”。");
            return false;
        }
        return true;
    }


    //添加distinct()
    Array.prototype.distinct = function() {
        var obj = {};
        for (var i = 0; i < this.length; i++) {
            var item = this[i].name.toLowerCase();
            if (!obj.hasOwnProperty(item)) {
                obj[item] = item;
            }
        }
        return obj;
    };



    function checkTableColumn() {
        $("#tc_columnStatus").html("");
        tc_columns= [];
        var tc_column = new Object();
        for (var i = 1; i < tableAddColumnId; i++) {
            tc_column = new Object();
            if (checkColumnNameCreate($("#tc_tableColumnName" + i).val().trim())) {
                tc_column["name"] = $("#tc_tableColumnName" + i).val().trim();
            }
            else {
                return false;
            }
            tc_column["type"] = $("#tc_tableColumnType" + i).find("option:selected").text();
//            if ($("#tc_tableColumnCompression" + i).val() != 0)
//                tc_column["compression"] = $("#tc_tableColumnCompression" + i).find("option:selected").text();
            tc_columns[i-1] = tc_column;

        }
        var tmp_col = tc_columns.distinct();
        if (Object.getOwnPropertyNames(tmp_col).length < tc_columns.length) {
            $("#tc_columnStatus").html("列名不可以重复。");
            return false;
        }
        return true;
    }


    function configKeyColumn() {
		if (!checkTableColumn()) {
			return;
		}
		else {
//		    var j=0;

			var cfgKeyCol_datas = [];

            for (var i = 1; i < tableAddColumnId; i++) {
                //	.trim()去除两端的空格
                cfgKeyCol_datas[i-1] = {"name":$('#tc_tableColumnName' + i).val().trim()};

            }

            if (tableAddColumnId == 0) {
				$("#tc_columnStatus").html("请至少创建一个列。");
				return;
			}



            $('#configKeyTable').bootstrapTable('destroy').bootstrapTable({
				data: cfgKeyCol_datas,
                classes: 'table',
                striped: true,
                formatNoMatches: function () {
                    return '';
                },
            });
            $('#HashTable').bootstrapTable('destroy').bootstrapTable({
                classes: 'table',
                striped: true,
                formatNoMatches: function () {
                    return '';
                },
            });
            $('#RangeTable').bootstrapTable('destroy').bootstrapTable({
                classes: 'table',
                striped: true,
                formatNoMatches: function () {
                    return '';
                },
            });
		}



        $("#myCarousel").carousel('next');
        $("#wizard-step1").attr("class", "complete");
        $("#wizard-step2").attr("class", "complete");
        $("#wizard-step3").attr("class", "active");
    }


    function addTableColumn() {
        $("#tc_columnStatus").html("");
        addColumn(tableAddColumnId);
        tableAddColumnId++;
    }
//加列
    function addColumn(id) {
        var htmltext = '';
        htmltext += '<div class="tablebg2" id="tc_tableColumn' + id + '" style="width:650px;height:40px;margin-left:10px;margin-bottom:5px;">'
            + '<div class="r_box" style="width:130px;">'
            + '<input name="tc_tableColumnName' + id + '" id="tc_tableColumnName' + id + '" type="text" class="textbox1" style="width:130px;height:25px;">'
            + '</div>'
            + '<div class="r_box" style="width:80px;">'
            + '<select name="tc_tableColumnType' + id + '" id="tc_tableColumnType' + id +'" style="width:80px;height:25px;" onchange="onSelectColumnType(' + id + ')">'
            + '<option value="0">int8</option>'
            + '<option value="1">int16</option>'
            + '<option value="2">int32</option>'
            + '<option value="3">int64</option>'
            + '<option value="4">float</option>'
            + '<option value="5">double</option>'
            + '<option value="6">bool</option>'
            + '<option value="7">string</option>'
            + '</select></div>'
//            + '<div class="r_box" style="width:80px;">'
//            + '<input name="tc_tableColumnDesc' + id + '" id="tc_tableColumnDesc' + id + '" type="text" class="textbox1" style="width:80px;height:25px;">'
//            + '</div>'
            + '<div class="r_box" style="width:30px;text-align:center">'
            + '<input name="tc_tableColumnPk' + id + '" id="tc_tableColumnPk' + id + '" type="checkbox" onclick="disableSeq(' + id + ')"  title="注意：一旦创建主键不可更改！">'
            + '</div>'
            + '<div class="r_box" style="width:30px;">'
            + '<input name="tc_tableColumnSeq' + id + '" id="tc_tableColumnSeq' + id + '" type="text" class="textbox1" style="width:30px;height:25px;" disabled="disabled">'
            + '</div>'
            + '<div class="r_box" style="width:40px;padding-top:3px;text-align:left;">'
            + '<span class="glyphicon glyphicon-plus" title="增加列" onclick="addTableColumn();"></span>&nbsp;&nbsp;'
            + '<span class="glyphicon glyphicon-minus" title="删除列" onclick="removeTableColumn(this);"></span></div>'
            + '</div>';
        $("#tc_lastTableColumn").before(htmltext);
    }

    function removeTableColumn(obj) {
        debugger
        $("#tc_columnStatus").html("");
        $('#tc_tableColumn' + (tableAddColumnId-1)).remove();
//        $(obj).parents(".tablebg2").remove();//parents不可靠
//        $(obj).closest(".tablebg2").remove();//id
        tableAddColumnId--;//不用减id
    }


    function disableSeq(id) {
        if ($("#tc_tableColumnPk" + id).attr("checked")) {
            $('#tc_tableColumnSeq' + id).removeAttr("disabled");
        }
        else {
            $('#tc_tableColumnSeq' + id).attr("value","");
            $('#tc_tableColumnSeq' + id).attr("disabled", "disabled");
        }
    }

    function tableCreate(){
        debugger;
        if (!checkTableColumn()) {
            return;
        }
        else {
//            $("#tableCreateStatus").html("");
            var tableCreateMap = {};
//
//            //表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
//            if(!isValidName($("#tc_tableName").val()))
//            {
//                $("#tableCreateStatus").html("");
//                $("#tableCreateStatus").append("创建表错误: " + "表名只能包含拉丁字母或数字或下划线“_”，且只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。");
//                return;
//            }
//        tableCreateMap["primary_key_type"] = $("#keyTypeTableCreate").val();
//        tableCreateMap["hash_key_type"] = $("#hashKeyTypeTableCreate").val();

            tableCreateMap["description"] = tc_tabledesc;

            var url = "/otscfgsvr/api/table/";
            var ots_tableName = $("#tc_tableName").val();
            url += ots_tableName;
//            alert("创建表");
//            return;

        $.ajax({
            type: "POST",
            url: url,
            data: JSON.stringify(tableCreateMap),
            dataType: "json",
            contentType: "application/json",
            timeout: 30000,
            success: function (results, msg) {
                debugger;
                $("#tableCreateStatus").html("");
                $("#tableCreateStatus").append("创建表").append(errorInfo(results["errcode"]));
                if(results["errcode"] == 0)
                    getTableInfo();
					$("#tc_forward2");removeAttr("disabled");
					$("#tc_forward2").css("background-color","white");
					$("#wizard-step3").attr("class", "complete");
            },
            error: function(msg){
                $("#tableCreateStatus").html("");
                var errmsg = "创建表失败！错误: " + getStructMsg(msg);
                if (errmsg.indexOf("Forbidden") >= 0)
                    errmsg += "（用户缺失创建表的权限！）";
                $("#tableCreateStatus").append(errmsg);
            }
        });

        }


    }


    function goBack(step) {
        clearTcStatus();
        var preStep = step - 1;
        $("#wizard-step" + preStep).attr("class", "active");
        $("#wizard-step" + step).removeClass("active");
    }



//    $('#rangeBound').on("show.bs.modal", function () {
//        $(this).removeData("bs.modal");
//    });
//    function clickRangeBound(row, index) {
//        clearPartitionStatusInfo();
//        modal_row = row;
//        modal_index = index;
//        modal_partition_type = partitionType.value;
//        $("#rangeBound").modal({
//            backdrop: "static",
//            show: false,
//            remote: "table_range_bound.jsp"
//        });
//    }

    function clickHiveColType(element) {
        $("#tc_columnStatus").html("");
        var colSeq = $(element).parent().parent().attr("data-col-seq");
        var value = $(element).val();

        // To set length & scale
        switch(value) {
            case "7":
                $("#tc_hiveColLength" + colSeq).show();
                $("#tc_hiveColLength" + colSeq).removeAttr("required");
                $("#tc_hiveColLength" + colSeq).attr("max", "38");
                $("#tc_hiveColScale" + colSeq).show();
                break;
            case "8":
                $("#tc_hiveColLength" + colSeq).show();
                $("#tc_hiveColLength" + colSeq).attr("required", true);
                $("#tc_hiveColLength" + colSeq).attr("max", "255");
                $("#tc_hiveColScale" + colSeq).hide();
                break;
            case "9":
                $("#tc_hiveColLength" + colSeq).show();
                $("#tc_hiveColLength" + colSeq).attr("required", true);
                $("#tc_hiveColLength" + colSeq).attr("max", "65535");
                $("#tc_hiveColScale" + colSeq).hide();
                break;
            default:
                $("#tc_hiveColLength" + colSeq).hide();
                $("#tc_hiveColScale" + colSeq).hide();
                break;
        }
    }

    function getHiveColTypeText(dataTypeValue, colSeq) {
        // 7-decimal, 8-char, 9-varchar
        var dataTypeText = $("#tc_hiveColType" + colSeq + " :selected").text();
        var length = 0;
        var scale = 0;
        switch(dataTypeValue) {
            case "7":
                if ($("#tc_hiveColLength" + colSeq).val()) {
                    length = $("#tc_hiveColLength" + colSeq).val().trim();
                    dataTypeText = dataTypeText + "(" + length;
                    if ($("#tc_hiveColScale" + colSeq).val()) {
                        scale = $("#tc_hiveColScale" + colSeq).val().trim();
                        dataTypeText = dataTypeText + "," + scale;
                    }

                    dataTypeText = dataTypeText + ")";
                }
                break;
            case "8":
                length = $("#tc_hiveColLength" + colSeq).val().trim();
                dataTypeText = dataTypeText + "(" + length + ")";
                break;
            case "9":
                length = $("#tc_hiveColLength" + colSeq).val().trim();
                dataTypeText = dataTypeText + "(" + length + ")";
                break;
            default:
                break;
        }
        return dataTypeText;
    }

    function checkHiveColumn() {
        $("#tc_columnStatus").html("");
        tc_columns = [];
        tc_columns_hive_partition = [];
        var listParititonColSeq = [];

        $("#tc_hiveTableColumns").children(".tablebg2").each(function() {
            if ($(this).find("[data-name='tc_hiveColName']").val()){
                var colSeq = $(this).attr("data-col-seq");
                var tc_column = new Object();
                tc_column["name"] = $("#tc_hiveColName" + colSeq).val().trim();
                var dataTypeValue = $("#tc_hiveColType" + colSeq + " :selected").val();
                tc_column["type"] = getHiveColTypeText(dataTypeValue, colSeq);
                tc_column["description"] = strTrim($("#tc_hiveColDesc" + colSeq).val());

                tc_columns.push(tc_column);
            }
        });

        var tmp_col = tc_columns.distinct();
        if (Object.getOwnPropertyNames(tmp_col).length < tc_columns.length) {
            $("#tc_columnStatus").html("列名：不可以重复");
            return false;
        }
        return true;
    }

    function addHiveTableColumn() {
        $("#tc_columnStatus").html("");
        $(templateHiveCol(hiveTableAddColumnId++)).appendTo("#tc_hiveTableColumns");
    }

    function removeHiveColumn(element) {
        $(element).parent().parent().remove();
        $("#tc_columnStatus").html("");
    }

    function checkHiveDelimiter() {
        var regex_contain_number = /^[0-9]*$/;
        if ($("#fieldDelimiterInput").val()) {
            tc_field_delimiter = $("#fieldDelimiterInput").val();
            if (tc_field_delimiter.indexOf("\\") == 0) {
                tc_field_delimiter = decodeDelimiter(tc_field_delimiter);
                if (tc_field_delimiter == $("#fieldDelimiterInput").val()) {
                    $("#tc_partitionStatus").html("非法的字段分隔符：请参考文档。");
                    return false;
                }
            } else {
                if (tc_field_delimiter.length > 1) {
                    $("#tc_partitionStatus").html("非法的字段分隔符：仅支持单字节。");
                    return false;
                }
                if (regex_contain_number.test(tc_field_delimiter)) {
                    $("#tc_partitionStatus").html("非法的字段分隔符：不支持数字。");
                    return false;
                }
            }
        }
        else {
            tc_field_delimiter = null;
        }

        return true;
    }

    function clearTcStatus() {
        $("[data-name=tc_status]").html("");
    }
</script>



