<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%>
<%@ page language="java" import="com.baosight.xinsight.ots.cfgsvr.bean.AuthBean" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
	<meta charset="UTF-8"/>
	<meta http-equiv="X-UA-Compatible" content="IE=edge">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>表详细页--OTS 配置中心</title>
	<link rel="shortcut icon" href="jsp/images/icon.ico"/>
	<link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
	<link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
	<link href="jsp/css/style.css" rel="stylesheet" type="text/css">
	<link href="jsp/css/content-style.css" rel="stylesheet" type="text/css">

	<script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
	<script type="text/javascript" src="jsp/js/menu-anim.js"></script>
	<script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
	<script type="text/javascript" src="jsp/js/bootstrap-table.js"></script>
	<script type="text/javascript" src="jsp/My97DatePicker/WdatePicker.js"></script>
	<script type = "text/javascript" src="jsp/js/exceptionDic.js"></script>
	<script type="text/javascript" src="jsp/js/init-js.js"></script>
	<script type="text/javascript" src="jsp/js/common.js"></script>

	<script type="text/javascript">
        $(function(){
            showActivePage("table");
        });
	</script>
</head>

<body class="body_fix">
<div id="header">
	<jsp:include page="header.jsp" />
</div>
<div class="content" >
	<%
		String tenantId = "-1"; //means tenant id is empty
		String token = "";
		String tableId = "-1";  //means not need table id
		String tenant = "";
		String username = "";
		String userId = "-1";  //
		if(session.getAttribute("tenant")!=null)
		{
			token = session.getAttribute("token").toString();
			tenant = session.getAttribute("tenant").toString();
			username = session.getAttribute("username").toString();
			tenantId = session.getAttribute("tenantId").toString();
			userId = session.getAttribute("userId").toString();
			tableId = request.getParameter("id");
		}
	%>
	<!---content--->
	<div class="row" >
		<div class="row" style="height:40px;">
			<div class="pageicon">|</div>
			<div style="margin-left:5px;margin-top:5px;">
				<ul class="breadcrumb pagetitle" style="background-color:transparent;margin-bottom:0px;padding-top:0px;padding-left:5px;">
					<li><a href="table.jsp" >OTS-表的管理</a></li>
					<li><a href="#" id="tablename"></a></li>
				</ul>
			</div>
		</div>
		<div class="row" style="margin-top:0px;">
			<div id="menuTop" class="col-md-9" style="min-height:35px;width:100%">
				<div id="menuType" class="menuType">
					<div id="dataInfo" class="contentMenu-sel" onclick="clickData();">表数据</div>
					<div id="columnInfo" class="contentMenu-col" onclick="clickColumnInfo();">列信息</div>
					<div id="indexInfo" class="contentMenu" onclick="clickIndex();">索引</div>
				</div>
			</div>
		</div>
		<hr style="height: 1px; background: #aaa;  margin-left:70px;margin-right:70px;margin-top:0px;margin-bottom:10px;" />
		<div id="pageGroup">
			<div  id="dataPage">

				<div class="row" style="margin-left:70px;margin-top:0px;margin-right:70px;height:30px;">
					<span class="r_text1"></span>主键列：
					<span>
					  <select id="PrimaryKeyItem" name="PrimaryKeyItem" size="1" class="sel" onChange="checkQuerySeq()">
						   <option>-请按顺序选择主键列-</option>
						   <option value="1">col1</option>
						   <option value="2">col2</option>
						   <option value="3">col3</option>
					   </select>
					</span>
				</div>
				<div class="row" style="margin-left:70px;margin-top:0px;margin-right:70px;">
					<input name="KeyQueryType" id="AccurateKeyRadio" type="radio" class="" value="0" checked>精确查询：
					<input name="" id="tableDetailAccurateKey" type="text" class="" style="width:120px;margin-top:5px;" value="" placeholder="精确值 " onclick="textFocus(0);">
					<input name="AccurateConfirm" type="button" class="btn_confirmA" class="" style="width:50px;margin-left: 10px;" value="确定" onclick='ConfirmAccurateQuery();'></br>
					<input name="KeyQueryType" id="rangeStartEndKeyRadio" type="radio" class="" value="1">范围查询：
					<input name="" id="tableDetailRangeStartKey" type="text" class="" style="width:120px;text-align:left;margin-top:5px;" value="" placeholder="起始键 " onclick="textFocus(1) ;">
					<input name="" id="tableDetailRangeEndKey" type="text" class="" style="width:120px;margin-left: 10px;margin-top:5px;" value="" placeholder="终止键 " onclick="textFocus(1)  ;">
					<input name="RangeConfirm" type="button" class="btn_confirmR" class="" style="width:50px;margin-left: 10px;" value="确定" onclick='ConfirmRangeQuery();'>
				</div>

				<%--<div class="row" style="margin-left:70px;margin-top:50px;margin-right:70px;height:30px;">--%>
				<%--<div class="r_text1" id="PrimaryKeyQueryItem">主键查询值：</div>--%>
				<%--</div>--%>

				<div  id="PKQueryDiv" style="margin-left:70px;margin-top:10px;overflow-y:auto;height:231px;width:38%;float: left;margin-right: 30px">
					<table id="PKQueryTable" style="height:231px;width:38%;float: left;table-layout:fixed; word-wrap:break-word;border-collapse:collapse;width:100%;">
						<thead>
						<tr>
							<th data-field="PKQueryName" data-align="left" >主键查询列名&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
							<th data-field="PKQueryValue" data-align="left" >主键查询列值&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th>
							<th data-field="PKQueryValueOperate" data-align="left"   data-formatter="PKQueryOperateFormatter" data-events="operateEvents">操作</th>
						</tr>
						</thead>
					</table>
				</div>

				<div>
					<input name="" type="button" class="btn2" class="" style="width:50px;margin-top:50px;margin-left:0px;" value="查询" onclick='recordQuery();'>
					<div class="r_text14" id="noQueryItem" style="margin-left:50px;margin-top:0px;height:15px;">请输入完整查询条件。</div>
					<div class="r_text14" id="PKQ_infoStatus" data-name="hr_status" style="margin-left:10px;width:550px;text-align:center;" ></div>
				</div>
				<%--<div class="row" style="margin-left:70px;margin-top:0px;margin-right:70px;height:15px;">--%>
				<%--<div class="r_text14" id="noQueryItem" style="margin-left:60px;">请输入完整查询条件。</div>--%>
				<%--</div>--%>
				<div class="row" style="margin-left:70px;margin-top:0px;margin-right:70px;height:40px;">
					<div id="tableDetailRangeTips" style="text-align:right;color:#428BCA;">注：当选择精确查询时，每个列值必须完整；</br>当选择范围查询且范围查询不是第一个列时，范围查询之前的必须是完整的精确值，如此查询才有效！</div>
				</div>

				<div class="row" style="margin-left:70px;margin-right:70px;margin-top:5px;">
					<jsp:useBean id="test" class="com.baosight.xinsight.ots.cfgsvr.bean.AuthBean" />
					<jsp:setProperty name="test" property="token" value = "<%=token %>" />
					<jsp:setProperty name="test" property="tenant" value = "<%=tenant %>" />
					<jsp:setProperty name="test" property="username" value = "<%=username %>" />
					<jsp:setProperty name="test" property="tenantId" value = "<%=tenantId %>" />
					<jsp:setProperty name="test" property="userId" value = "<%=userId %>" />
					<jsp:setProperty name="test" property="serviceName" value = "OTS" />
					<jsp:setProperty name="test" property="tableId" value = "<%=tableId %>" />

					<div class="table-controls">
						<div class="pull-right">
							<%--<c:if test="${test.hasRecordWritePerm}">--%>
							<input name="" type="button" class="btn" style="width:80px;" value="添加记录" onclick='clickAddRecord();' data-toggle="modal"  data-target="#recordCreate">
							<input name="" type="button" class="btn" style="width:80px;" value="清空记录" onclick='clickClearRecord();'>
							<span class="dropdown">
		      					  <button type="button" class="btn dropdown-toggle" data-toggle="dropdown">批量删除<span class="caret"></span></button>
			      				  <ul class="dropdown-menu" role="menu">
									  <li><a href="#" class="recordMultiDelete" onclick='clickDeleteMultiRecords();' data-method="remove">删除已选行</a></li>
									  <li><a href="#" onclick='clickStrategyDelete();' data-toggle="modal"  data-target="#strategyDelete">按策略删除</a></li>
								  </ul>
							   </span>
							<%--</c:if>--%>
							<input name="" type="button" class="btn" style="width:100px;" value="编辑显示列" onclick='clickEditDisplayColumns();' data-toggle="modal"  data-target="#propertyEdit">
						</div>
					</div>

					<div id="table">
						<table id="tableDetailRecord" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
							<thead>
							<tr>
								<th data-field="state" data-checkbox="true"></th>
								<th data-field="PK_column" data-align="center">主键列</th>
								<th data-field="" data-align="center"></th>
								<th data-field="" data-align="center"></th>
								<th data-field="" data-align="center"></th>
								<th data-field="" data-align="center"></th>
								<th data-field="" data-align="center"></th>
								<th data-field="" data-align="center"></th>
								<th data-class="col-operate-record" data-field="operate" data-align="center" data-formatter="recordOperateFormatter" data-events="operateEvents">操作</th>
							</tr>
							</thead>
						</table>
					</div>
				</div>
			</div>

			<div class="row" id="columnPage" style="margin-left:70px;margin-right:70px;display: none" >
				<div>
					<table id="tableColumn" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
						<thead>
						<tr>
							<th class="" data-field="state" data-checkbox="true"></th>
							<th data-field="name" data-align="center">列名</th>
							<th data-field="type" data-align="center">类型</th>
							<th data-field="ifpk" data-align="center">是否主键</th>
							<th data-field="pk_seq" data-align="center">主键排序</th>
						</tr>
						</thead>
					</table>
				</div>
			</div>

			<div class="row" id="indexPage" style="margin-left:70px;display: none" >
				<div class="row" style="margin-top:5px;">
					<span>索引名：&nbsp;&nbsp;&nbsp;<input name="" id="indexQueryName" type="text" value="">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>
					<span><input name="" type="button" class="btn2" style="width:50px" value="查询" onclick='indexQuery();'></span>
				</div>
				<div class="row" style="margin-right:70px;margin-top:5px;">
					<div style="text-align:right;color:#428BCA;">注：索引操作需要处理时间稍长，操作间隔建议不要过频！</div>
					<%--<c:if test="${test.hasRecordWritePerm}">--%>
					<div class="table-controls">
						<div class="pull-right">
							<input name="" type="button" class="btn" style="width:80px" value="新建索引" onclick='clickCreateIndex();' data-toggle="modal"  data-target="#indexCreate">
							<input name="" type="button" class="btn" style="width:80px" value="批量删除" onclick='clickDeleteMultiIndex();' data-method="remove">
						</div>
					</div>
					<%--</c:if>--%>
					<table id="tableDetailIndex" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
						<thead>
						<tr>
							<th data-field="state" data-checkbox="true"></th>
							<th data-field="index_name" data-align="center" data-formatter="indexNameFormatter">索引名</th>
							<th data-field="type" data-align="center" data-formatter="indexTypeFormatter">类型</th>
							<th data-field="pattern" data-align="center" data-formatter="indexPatternFormatter">模式</th>
							<th data-field="create_time" data-align="center">创建时间</th>
							<th data-field="last_modify" data-align="center">最后修改时间</th>
							<th class="col-operate-index" data-field="operate" data-align="center" data-formatter="indexOperateFormatter" data-events="operateEvents">操作</th>
						</tr>
						</thead>
					</table>
				</div>
			</div>

		</div>
	</div>
</div>
<!---dialog: record--->
<div class="modal text-center" id="recordCreate" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<div class="modal text-center" id="recordEdit" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<div class="modal text-center" id="strategyDelete" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<!---dialog: property--->
<div class="modal text-center" id="propertyEdit" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<!---dialog: index--->
<div class="modal text-center" id="indexCreate" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<div class="modal text-center" id="indexEdit" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog" style="display: inline-block; width: auto;">
		<div class="modal-content"></div>
	</div>
</div>
<div>
	<jsp:include page="footer.jsp" />
</div>
<div>
	<jsp:include page="errorTips.jsp" />
</div>
</body>

<script type="text/javascript">
    var tableDetailRecordNum = 100;		    //每次查询默认返回记录数
    var table_detail_row_index = 0;
    var total_record_num = 0;				//已经返回记录数
    var table_detail_records = [];
    var table_detail_indexes = [];
    var args_tablename = getUrlParam("tablename");
    //    	var args_pktype = getUrlParam("pktype");
    //    	var args_range = getUrlParam("range");
    var args_id = getUrlParam("id");
    var display_name = getUrlParam("display");
    var table_detail_show_columns_num = 8;
    var table_detail_show_columns = [];
    var table_detail_query_columns = [];
    var table_detail_column_property = {"field":"", "title":"", "align":"center"};
    var table_detail_record_query_condition={};
    var modal_table_name;
    var modal_index_name;
    var modal_index_info = {};
    var modal_hash_key;
    var modal_range_key;
    var timeoutId;
    //        //todo 模拟数据
    //        var wls_PKQ_datas = [];
    //        wls_PKQ_datas[0]={"PKQueryName":"colName3","PKQueryValue":"colValue3"};
    //        wls_PKQ_datas[1]={"PKQueryName":"colName1","PKQueryValue":"colValue1"};
    //        wls_PKQ_datas[2]={"PKQueryName":"colName5","PKQueryValue":"colValue5"};

    function recordAlertMsg(msg){
        errorAlertMsg(msg);
    }

    function recordConfirmAlert(msg, operate, param) {
        confirmAlertMsg(msg, 1, operate, param);
    }

    function indexConfirmAlert(msg, operate, param) {
        confirmAlertMsg(msg, 2, operate, param);
    }


    //主键查询值的操作,目前只支持删除
    function PKQueryOperateFormatter(value, row, index) {
        return [
            '<input name="" type="button" class="keyDeletePKQ" style="width:40px;" value="删除" data-method="remove" data-target="#PKQueryTable">',

        ].join('');
    }


    function clickDeletePKQ(editRow) {
        debugger

        $("#PKQueryTable").bootstrapTable('remove',{
            field: 'PKQueryName',
            values: [editRow.PKQueryName]
        });
        $("#PKQ_infoStatus").html("若删除的不是最后一个主键列，请再填写中间的主键列！");
    }

    $(function(){
//       		if (args_tablename == null || args_pktype == null) {
//       			recordAlertMsg("非法的URL，请跳转至“表的管理”页面，点击表名，进入“表详细页”页面。");
//       		 	//window.location = "table.jsp";
//       		}

        $("#tablename").text(getStructName(args_tablename) + "-数据");
        document.getElementById("tablename").title = args_tablename;
        if (display_name == "index") {
            clickIndex();
        }
        for (var i=0; i<table_detail_show_columns_num; i++) {
            table_detail_show_columns[i] = table_detail_column_property;
        }
        table_detail_show_columns[0] = {"field":"state", "checkbox":true};
//       		table_detail_show_columns[1] = {"field":"hash_key", "title":"主键列", "align":"center"};
//            table_detail_show_columns[2] = {"field":"range_key", "title":"普通键列", "align":"center"};

//       		if (args_pktype == 1) {
//       			table_detail_show_columns[2] = {"field":"range_key", "title":"Range键", "align":"center"};
//       			document.getElementById("tableDetailRangeQuery").style.display = "block";
//       			document.getElementById("tableDetailHashQuery").style.display = "none";
//       			document.getElementById("tableDetailRangeTips").style.display = "block";
//       			if (args_range != null && args_range != 0) {
//       				$("#tableDetailRangePrefixKey").attr("disabled","disabled");
//       			}
//       		}

//       		// todo 模拟hash键列
////            $('#HashKeyTable').bootstrapTable('destroy').bootstrapTable({
//            $('#HashKeyTable').bootstrapTable({
//                data: wls_hash_datas,
//                classes: 'table',
//                striped: true,
//                formatNoMatches: function () {
//                    return '';
//                },
//            });
//
//            // todo 模拟Range键列
////            $('#RangeKeyTable').bootstrapTable('destroy').bootstrapTable({
//            $('#RangeKeyTable').bootstrapTable({
//                data: wls_range_datas,
//                classes: 'table',
//                striped: true,
//                formatNoMatches: function () {
//                    return '';
//                },
//            });
        // todo 模拟主键查询键值
        $('#PKQueryTable').bootstrapTable('destroy').bootstrapTable({
//                data: wls_PKQ_datas,
            data: [],
            classes: 'table',
            striped: true,
            formatNoMatches: function () {
                return '';
            },
        });

        $('#tableDetailRecord').bootstrapTable({
            data: [],
            classes: 'table',
            striped: true,
            formatNoMatches: function () {
                return '';
            },
            columns: table_detail_show_columns
        });
        recordInit();
        $('#tableDetailRecord').on('page-change.bs.table', function (e, size, number){
            var $table = $('#tableDetailRecord').bootstrapTable();
            var totalPages = $table.bootstrapTable('getOptions').totalPages;
            if(totalPages == size){
                total_record_num = $table.bootstrapTable('getOptions').totalRows;
                tableDetailRecordUpdate();
            }
        });
    });

    function recordInit(){
        $.ajax({
            cache: false,
            type: "GET",
            url: "/otscfgsvr/api/table/" + args_tablename + "/display_columns",
            dataType: "json",
            timeout:30000,
            success:function(results, msg) {
                if (results["errcode"] == 0) {
                    var property = results["columns"];
                    var showPosition = property.indexOf("\"propertyToShow\"");
                    table_detail_query_columns = property.substring(showPosition+18, property.length-1).split(",");
                    getTableDetailShowColumns();
                    getRecordList();
                }
                else {
                    recordAlertMsg("获取表 " + args_tablename + " 显示列信息失败！错误: " + results["errcode"]);
                }
            },
            error: function(msg){
                if(msg["status"] != 404) {
                    var errmsg = "获取表" + getStructName(args_tablename) + " 显示列信息错误: " + getStructMsg(msg);
                    recordAlertMsg(errmsg);
                }
                getRecordList();
            }
        });
    }

    function clickData()
    {
        $("#tablename").text(getStructName(args_tablename) + "-数据");
        document.getElementById("tablename").title = args_tablename;
        $("#index_name").css("display", "none");
        menuClick(0,3);

        $('#tableDetailRecord').bootstrapTable({
            data: [],
            classes: 'table',
            striped: true,
            formatNoMatches: function () {
                return '';
            },
        });

        $("#tableDetailHashKey").attr("value", "");
        $("#tableDetailRangePrefixKey").attr("value", "");
        $("#tableDetailRangeStartKey").attr("value", "");
        $("#tableDetailRangeEndKey").attr("value", "");

        recordQuery();
    }

    function clickColumnInfo()
    {
        $("#tablename").text(getStructName(args_tablename) + "-列信息");
        document.getElementById("tablename").title = args_tablename;
        $("#index_name").css("display", "none");
//            $("#index_name").css("display", "inline-block");
        menuClick(1,3);
        $('#tableColumn').bootstrapTable({
            data: [],
            classes: 'table',
            striped: true,
            formatNoMatches: function () {
                return '';
            },
        });
        $('#tableColumn').on('pre-body.bs.table', function (data, rows) {
            clearTimeout(timeoutId);
            indexGetStatus(rows);
        });
        indexInit();
    }

    function checkQuerySeq()
    {
        debugger;
        $("#PKQ_infoStatus").html("");
        var select = $("#PrimaryKeyItem");
        var index = select.get(0).selectedIndex;
//            var PKItemSeq = $("#PrimaryKeyItem").val();
        var count = 0;
        var PKQuery_table = [];
        PKQuery_table = $("#PKQueryTable").bootstrapTable('getData');
        //获得第index-1个option的文本内容：
        var text_before = select.get(0).options[index-1].text;

        if (index == 1) {
//                $("tableDetailAccurateKey").removeAttr("disabled");//
//                $("tableDetailAccurateKey").attr("disabled",false);
            $("#tableDetailAccurateKey").removeAttr("readonly");
            $("#tableDetailRangeStartKey").removeAttr("readonly");
            $("#tableDetailRangeEndKey").removeAttr("readonly");
//                return;
        }
        else if(index>1 && index <= select.get(0).length)
        {
            for (var i in PKQuery_table) {
                if (PKQuery_table[i].PKQueryName == text_before) {
//                        $("tableDetailAccurateKey").removeAttr("disabled");
//                        $("tableDetailAccurateKey").attr("disabled",false);//
                    $("#tableDetailAccurateKey").removeAttr("readonly");
                    $("#tableDetailRangeStartKey").removeAttr("readonly");
                    $("#tableDetailRangeEndKey").removeAttr("readonly");
                    break;
                }
                else{
                    count++;
                }
            }
            if ( (count) == PKQuery_table.length){
                $("#PKQ_infoStatus").html("");
                $("#PKQ_infoStatus").append("请按顺序填写查询的主键值。");
//                    $("#tableDetailAccurateKey").attr("disabled",true);//当用这个方法时，无法将不可编辑的状态变为可编辑状态
                $("#tableDetailAccurateKey").attr("readonly","readonly");
                $("#tableDetailRangeStartKey").attr("readonly","readonly");
                $("#tableDetailRangeEndKey").attr("readonly","readonly");
//                    return;
            }
        }

        if((index+1) < select.get(0).length)
        {
            var colName_text_after = select.get(0).options[index+1].text;
            for (var i in PKQuery_table) {
                if (PKQuery_table[i].PKQueryName == colName_text_after) {
                    textFocus(0);
                    $("#tableDetailAccurateKey").removeAttr("readonly");
                    $("#tableDetailRangeStartKey").attr("readonly","readonly");
                    $("#tableDetailRangeEndKey").attr("readonly","readonly");
                    $("#PKQ_infoStatus").html("此时该主键列只能支持精确查询（因为后面还存在主键列）。");
                    break;
                }
            }
        }
        return;


    }

    function ConfirmAccurateQuery()
    {
        debugger;

        var select = $("#PrimaryKeyItem");
        var index = select.get(0).selectedIndex;
        var PKQuery_table = [];
        //获得第index-1个option的文本内容：
        var colName_text_now = select.get(0).options[index].text;//得到选择项的内容
        var colName_text_before = select.get(0).options[index-1].text;
        var colValue_data = $("#tableDetailAccurateKey").attr("value");//得到文本框中的列值
        var accurate_data = {"PKQueryName":colName_text_now,"PKQueryValue":colValue_data};
        PKQuery_table = $("#PKQueryTable").bootstrapTable('getData');
        if (index == 1) {
//	     	判断表中是否已存在，若存在，就更新数据，否则，新插一行。
            if (PKQuery_table.length !=0 &&PKQuery_table[index-1].PKQueryName == colName_text_now) {
                $("#PKQueryTable").bootstrapTable('updateRow',{index:(index-1), row:accurate_data});
            }else{
                $("#PKQueryTable").bootstrapTable('insertRow', {index:(index-1), row:accurate_data});
            }
            return;
        }
        else if(index>1 && index <= select.get(0).length)
        {

            if (PKQuery_table[index-2].PKQueryName == colName_text_before) {
                if(PKQuery_table[index-2].PKQueryValue.indexOf("-") >= 0)
                {
                    $("#PKQ_infoStatus").html("填写失败。请保证之前的主键列都是精确查询。");
                }
                else if ( PKQuery_table.length >=index && PKQuery_table[index-1].PKQueryName == colName_text_now) {
                    $("#PKQueryTable").bootstrapTable('updateRow',{index:(index-1), row:accurate_data});
                }
                else {
                    $("#PKQueryTable").bootstrapTable('insertRow', {index:(index-1), row:accurate_data});
                    $("#tableDetailRangeStartKey").removeAttr("readonly");
                    $("#tableDetailRangeEndKey").removeAttr("readonly");

                }
            }
            else{
                $("#PKQ_infoStatus").html("请按顺序填写查询的主键值。");
            }
        }
        return;
    }


    function ConfirmRangeQuery()
    {
        debugger;
//            var colName_data = $("#PrimaryKeyItem").find("option:selected").text();
//            var colStartValue_data = $("#tableDetailRangeStartKey").attr("value");
//            var colEndValue_data = $("#tableDetailRangeEndKey").attr("value");
//            var colValue_data =colStartValue_data+"-"+ colEndValue_data;
//            var range_data = {"PKQueryName":colName_data,"PKQueryValue":colValue_data};
//            $("#PKQueryTable").bootstrapTable('append', range_data);//_data----->新增的数据

        var select = $("#PrimaryKeyItem");
        var index = select.get(0).selectedIndex;
        var PKQuery_table = [];
        PKQuery_table = $("#PKQueryTable").bootstrapTable('getData');

        //获得第index-1个option的文本内容：
        var colName_text_now = select.get(0).options[index].text;//得到选择项的内容，列名
        var colName_text_before = select.get(0).options[index-1].text;
        var colStartValue_data = $("#tableDetailRangeStartKey").attr("value");//得到文本框中的列值
        var colEndValue_data = $("#tableDetailRangeEndKey").attr("value");//得到文本框中的列值
        var colValue_data =colStartValue_data+"-"+ colEndValue_data;//得到文本框中的列值，起始键+终止键
        var range_data = {"PKQueryName":colName_text_now,"PKQueryValue":colValue_data};

        if (index == 1) {
//	     	判断表中是否已存在，若存在，就更新数据，否则，新插一行。
            if (PKQuery_table.length !=0 &&PKQuery_table[index-1].PKQueryName == colName_text_now) {
                $("#PKQueryTable").bootstrapTable('updateRow',{index:(index-1), row:range_data});
            }else{
                $("#PKQueryTable").bootstrapTable('insertRow', {index:(index-1), row:range_data});
            }
            return;
        }
        else if(index>1 && index <= select.get(0).length)
        {
            if ( PKQuery_table[index-2].PKQueryName == colName_text_before) {
                if(PKQuery_table[index-2].PKQueryValue.indexOf("-") >= 0)
                {
                    $("#PKQ_infoStatus").html("填写失败。请保证之前的主键列都是精确查询。");
                }
                else if (PKQuery_table.length >=index && PKQuery_table[index-1].PKQueryName == colName_text_now) {
                    $("#PKQueryTable").bootstrapTable('updateRow',{index:(index-1), row:range_data});
                }
                else {
                    $("#PKQueryTable").bootstrapTable('insertRow', {index:(index-1), row:range_data});
                }
            }
            else{
                $("#PKQ_infoStatus").html("请按顺序填写查询的主键值。");
            }
        }
        return;




    }


    function clickIndex()
    {
        $("#tablename").text(getStructName(args_tablename) + "-索引");
        document.getElementById("tablename").title = args_tablename;
        $("#index_name").css("display", "inline-block");
        menuClick(2,3);
        $('#tableDetailIndex').bootstrapTable({
            data: [],
            classes: 'table',
            striped: true,
            formatNoMatches: function () {
                return '';
            },
        });
        $('#tableDetailIndex').on('pre-body.bs.table', function (data, rows) {
            clearTimeout(timeoutId);
            indexGetStatus(rows);
        });
        indexInit();
    }

    function recordOperateFormatter(value, row, index) {
        return [
            '<c:if test="${test.hasRecordWritePerm}">',
            '<input name="" type="button" class="recordEdit btn3" style="width:50px;" value="编辑" data-toggle="modal"  data-target="#recordEdit">',
            '&nbsp;&nbsp;&nbsp;&nbsp;',

            '<input name="" type="button" class="recordDelete btn3" style="width:50px;" value="删除" data-method="remove">',
            '</c:if>'
        ].join('');
    }

    window.operateEvents = {
        'click .recordEdit': function (e, value, row, index) {
            clickEditRecord(row);
        },
        'click .recordDelete': function (e, value, row, index) {
            clickDeleteRecord(row);
        },
        'click .indexRebuild': function (e, value, row, index) {
            clickRebuildIndex(row, index);
        },
        'click .indexEdit': function (e, value, row, index) {
            clickIndexEdit(row, index);
        },
        'click .indexClear': function (e, value, row, index) {
            clickClearIndex(row, index);
        },
        'click .indexDelete': function (e, value, row, index) {
            clickDeleteIndex(row);
        },
        'click .keyDeletePKQ': function (e, value, row, index) {
            clickDeletePKQ(row);
        },


    };

    function getRecordList(){
        var queryUrl = "/otscfgsvr/api/record/" + args_tablename + "?";
        queryUrl += "query_from=0&limit=" + tableDetailRecordNum + "&offset=0";
        if (table_detail_record_query_condition["hash_key"]) {
            queryUrl += "&hash_key=" + table_detail_record_query_condition["hash_key"];
        }
//        		if (args_pktype == 1) {
//    	    		if (table_detail_record_query_condition["range_query_type"] == 0 && table_detail_record_query_condition["range_prefix_key"]) {
//    	    			queryUrl += "&range_key_prefix=" + table_detail_record_query_condition["range_prefix_key"];
//    	    		}
//    	    		else if (table_detail_record_query_condition["range_query_type"] == 1) {
//    	    			if (table_detail_record_query_condition["range_start_key"]) {
//    	    				queryUrl += "&range_key_start=" + table_detail_record_query_condition["range_start_key"];
//    	    			}
//    	    			if (table_detail_record_query_condition["range_end_key"]) {
//    	    				queryUrl += "&range_key_end=" + table_detail_record_query_condition["range_end_key"];
//    	    			}
//    	    		}
//    	    	}
        if (table_detail_query_columns.length != 0 && table_detail_query_columns[0] != "") {
            queryUrl += "&columns=" + table_detail_query_columns.join(",");
        }

        $.ajax({
            cache: false,
            type: "GET",
            url: queryUrl,
            dataType: "json",
            timeout:30000,
            success:function(results, msg){
                if (results["errcode"] == 0) {
                    table_detail_records = results["records"];
                    for (var i=0; i<table_detail_records.length; i++){
                        $.each(table_detail_records[i],function(j){
                            table_detail_records[i][j] = htmlEscape(table_detail_records[i][j]);
                        });
                    }
                }
                else {
                    recordAlertMsg("记录查询失败！错误: " + results["errcode"]);
                }
            },
            complete: function() {
                var $table = $('#tableDetailRecord').bootstrapTable();
                var pagesize = $table.bootstrapTable('getOptions').pageSize;
                $('#tableDetailRecord').bootstrapTable('destroy').bootstrapTable({
                    data: table_detail_records,
                    classes: 'table',
                    undefinedText: '',
                    striped: true,
                    pagination: true,
                    pageSize: pagesize,
                    pageList: [5, 10, 15],
                    formatRecordsPerPage: function (pageNumber) {
                        return sprintf('每页显示 %s 条记录', pageNumber);
                    },
                    formatShowingRows: function (pageFrom, pageTo, totalRows) {
                        return sprintf('第  %s 到 %s 条记录', pageFrom, pageTo);
                    },
                    columns: table_detail_show_columns
                });
            },
            error: function(msg){
                recordAlertMsg("记录查询失败！错误: " + getStructMsg(msg));
            }
        });
    }

    function tableDetailRecordUpdate(){
        table_detail_record_query_condition["offset"] = total_record_num;
        var queryUrl = "/otscfgsvr/api/record/" + args_tablename + "?";
        queryUrl += "query_from=0&limit=" + tableDetailRecordNum + "&offset=" + table_detail_record_query_condition["offset"];
        if (table_detail_record_query_condition["hash_key"]) {
            queryUrl += "&hash_key=" + table_detail_record_query_condition["hash_key"];
        }

        if (args_pktype == 1) {
            if (table_detail_record_query_condition["range_query_type"] == 0 && table_detail_record_query_condition["range_prefix_key"]) {
                queryUrl += "&range_key_prefix=" + table_detail_record_query_condition["range_prefix_key"];
            }
            else if (table_detail_record_query_condition["range_query_type"] == 1) {
                if (table_detail_record_query_condition["range_start_key"]) {
                    queryUrl += "&range_key_start=" + table_detail_record_query_condition["range_start_key"];
                }
                if (table_detail_record_query_condition["range_end_key"]) {
                    queryUrl += "&range_key_end=" + table_detail_record_query_condition["range_end_key"];
                }
            }
        }
        if (table_detail_query_columns.length != 0 && table_detail_query_columns[0] != "") {
            queryUrl += "&columns=" + table_detail_query_columns.join(",");
        }

        $.ajax({
            cache: false,
            type: "GET",
            url: queryUrl,
            dataType: "json",
            timeout:30000,
            success:function(results, msg){
                if (results["errcode"] == 0) {
                    table_detail_records = results["records"];
                    for (var i=0; i<table_detail_records.length; i++){
                        $.each(table_detail_records[i],function(j){
                            table_detail_records[i][j] = htmlEscape(table_detail_records[i][j]);
                        });
                    }

                    $('#tableDetailRecord').bootstrapTable('append', table_detail_records);
                }
                else {
                    recordAlertMsg("记录查询失败！错误: " + results["errcode"]);
                }
            },
            error: function(msg){
                recordAlertMsg("记录查询失败！错误: " + getStructMsg(msg));
            }
        });

    }

    function recordQuery(){
        var hashkey = $("#tableDetailHashKey").val().trim();
        if (hashkey) {
            table_detail_record_query_condition["hash_key"] = encodeURIComponent(hashkey);
        }
        else {
            table_detail_record_query_condition["hash_key"] = null;
        }
        //table_detail_record_query_condition["offset"] = 0;

        if (args_pktype == 1) {
            table_detail_record_query_condition["range_query_type"] = -1;
            var range_query_type = $("input[name='rangeKeyQueryType']:checked").val();
            if (args_range == 0 && range_query_type == 0) {
                var range_prefix_key = $("#tableDetailRangePrefixKey").val();
                table_detail_record_query_condition["range_query_type"] = 0;
                table_detail_record_query_condition["range_prefix_key"] = encodeURIComponent(range_prefix_key);
            }
            else if (range_query_type == 1) {
                var range_start_key = $("#tableDetailRangeStartKey").val().trim();
                var range_end_key = $("#tableDetailRangeEndKey").val().trim();
                table_detail_record_query_condition["range_query_type"] = 1;
                table_detail_record_query_condition["range_start_key"] = encodeURIComponent(range_start_key);
                table_detail_record_query_condition["range_end_key"] = encodeURIComponent(range_end_key);
            }
        }
        getRecordList();
    }

    function clickDeleteRecord(editRow) {
        var confirmText = "确定删除记录 Hash键=" + getStructName(editRow["hash_key"]);
        if (args_pktype == 1) {
            confirmText += ", Range键=" + getStructName(editRow["range_key"]);
        }
        confirmText += " ?";
        recordConfirmAlert(confirmText, 0, editRow);
    }

    function recordDelete(editRow) {
        var queryUrl = "hash_key=" + encodeURIComponent(htmlUnescape(editRow["hash_key"]));
        if (args_pktype == 1) {
            queryUrl += "&range_key=" + encodeURIComponent(htmlUnescape(editRow["range_key"]));
        }
        $.ajax({
            type:"DELETE",
            url:"/otscfgsvr/api/record/" + args_tablename + "?" + queryUrl,
            timeout:30000,
            success:function(results, msg){
                if(results["errcode"] == 0)
                    getRecordList();
                recordAlertMsg("删除记录" + errorInfo(results["errcode"]));
                return;
            },
            error: function(msg){
                var errmsg = "删除记录失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function clickDeleteMultiRecords() {
        var $table = $('#tableDetailRecord').bootstrapTable();
        var selects = $table.bootstrapTable('getSelections');
        if (selects.length == 0){
            recordAlertMsg("请选中至少一条记录。");
            return;
        }
        var msg = '确定删除选定的记录？';
        recordConfirmAlert(msg, 1, selects);
    }

    function recordMultiDelete(selects){
        $.map(selects, function (row) {
            var hashkey = htmlUnescape(row["hash_key"]);
            var queryUrl = "hash_key=" + encodeURIComponent(hashkey);
            if (args_pktype == 1) {
                var rangekey = htmlUnescape(row["range_key"]);
                queryUrl += "&range_key=" + encodeURIComponent(rangekey);
            }
            $.ajax({
                async: false,
                type:"DELETE",
                url:"/otscfgsvr/api/record/" + args_tablename + "?" + queryUrl,
                timeout:30000,
                success:function(results, msg){
                    recordAlertMsg("删除记录" + errorInfo(results["errcode"]));
                    if(results["errcode"] == 0){
                        getRecordList();
                    }
                    return;
                },
                error: function(msg){
                    var errmsg = "删除记录失败！错误: " + getStructMsg(msg);
                    recordAlertMsg(errmsg);
                    return false;
                }
            });
        });

    }

    function clickClearRecord() {
        var msg = '确定清空表内所有记录？';
        recordConfirmAlert(msg, 2, null);
    }

    function recordClear() {
        $.ajax({
            type:"DELETE",
            url:"/otscfgsvr/api/record/" + args_tablename + "/truncate",
            timeout:30000,
            success:function(results, msg){
                if (results.errcode != 0) {
                    recordAlertMsg("清空记录" + errorInfo(results["errcode"]));
                    return;
                }
                $table = $('#tableDetailRecord').bootstrapTable('destroy').bootstrapTable({
                    data: [],
                    classes: 'table',
                    striped: true,
                    formatNoMatches: function () {
                        return '';
                    },
                    columns: table_detail_show_columns
                });
                recordAlertMsg("清空记录成功！");
            },
            error: function(msg){
                var errmsg = "清空记录失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function indexInit(){
        $("#indexQueryName").attr("value", "");
        $.ajax({
            cache: false,
            type: "GET",
            url: "/otscfgsvr/api/index/" + args_tablename + "/_all_indexes_info",
            dataType: "json",
            timeout: 30000,
            success: function(results, msg){
                if (results["errcode"] == 0) {
                    var index_info_list = results["index_info_list"];
                    table_detail_indexes = index_info_list;
                    for (var i=0; i<index_info_list.length; i++){
                        if (!index_info_list[i]["type"]) {
                            table_detail_indexes[i]["type"] = 0;
                        }
                    }
                }
                else {
                    recordAlertMsg("查询所有索引信息" + errorInfo(results["errcode"]));
                }
            },
            complete: function()
            {
                var $table = $('#tableDetailIndex').bootstrapTable();
                var pagesize = $table.bootstrapTable('getOptions').pageSize;
                $('#tableDetailIndex').bootstrapTable('destroy').bootstrapTable({
                    data: table_detail_indexes,
                    classes: 'table',
                    undefinedText: '',
                    striped: true,
                    pagination: true,
                    pageSize: pagesize,
                    pageList: [5, 10, 15],
                    formatRecordsPerPage: function (pageNumber) {
                        return sprintf('每页显示 %s 条记录', pageNumber);
                    },
                    formatShowingRows: function (pageFrom, pageTo, totalRows) {
                        return sprintf('第  %s 到 %s 条记录', pageFrom, pageTo);
                    },
                });
            },
            error: function(msg){
                var errmsg = "查询所有索引信息失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function indexNameFormatter(value, row) {
        return '<a style="cursor:pointer;" onclick=\'goIndexView("' + value + '","' + row["type"] + '","'  + row["table_id"] + '");\'>' + value + '</a>';
    }

    function indexTypeFormatter(value) {
        if (value == 1) {
            return "Hbase";
        }
        else {
            return "ElasticSearch";
        }
    }

    //		function indexPatternFormatter(value) {
    //			switch(value) {
    //				case 0:
    //					return "离线";
    //				case 1:
    //					return "实时";
    //				default:
    //					return "-";
    //			}
    //		}

    function indexOperateFormatter(value, row, index) {
        return [
            '<div class="progress progress-striped" id="indexProgress' + index + '" style="margin-bottom:5px;"><div class="progress-bar" id="indexProgressBar' + index + '" role="progressbar" style="width:' + row["progress"] + '"></div></div>',
            '<div id="indexButton' + index + '" style="display:block">',
            '<div id="indexState' + index + '" class="r_text10" style="display:none">编译失败！</div>',
            '<c:if test="${test.hasRecordWritePerm}">',
            '<input name="" type="button" class="indexClear btn3" style="width:50px;" value="清空" data-method="updateRow">',
            '&nbsp;&nbsp;&nbsp;&nbsp;',
            '<input name="" type="button" class="indexRebuild btn3" style="width:50px;" value="重建" data-method="updateRow">',
            '&nbsp;&nbsp;&nbsp;&nbsp;',
            '<input name="" type="button" class="indexEdit btn3" style="width:50px;" value="编辑" data-toggle="modal"  data-target="#indexEdit">',
            '&nbsp;&nbsp;&nbsp;&nbsp;',
            '<input name="" type="button" class="indexDelete btn3" style="width:50px;" value="删除"></div>',
            '</c:if>'
        ].join('') ;
    }

    function indexGetStatus(editRows) {
        var progressId = "";
        var progressBarId = "";
        var progress = "";
        for (var i=0; i<editRows.length; i++){
            progressId = "indexProgress" + i;
            progressBarId = "indexProgressBar" + i;
            progress = editRows[i]["progress"];
            if (progress == null || (parseFloat(progress, 10) >= 0 && parseFloat(progress, 10) < 100.0)) {
                var query_condition = "index_type=" + editRows[i]["type"] + "&table_id=" + editRows[i]["table_id"] + "&index_id=" + editRows[i]["index_id"];
                $.ajax({
                    cache: false,
                    async: false,
                    type: "GET",
                    url: "/otscfgsvr/api/index/status/" + args_tablename + "/" + editRows[i]["index_name"] + "?" + query_condition,
                    dataType: "json",
                    timeout: 20000,
                    success: function(results, msg){
                        editRows[i]["progress"] = results["progress"];
                        progress = editRows[i]["progress"];
                        if (document.getElementById(progressId)) {
                            if (parseFloat(progress, 10) >= 0 && parseFloat(progress, 10) < 100.0) {
                                document.getElementById("indexButton" + i).style.display = "none";
                                document.getElementById(progressId).style.display = "block";
                                document.getElementById(progressBarId).style.width = editRows[i]["progress"] + "%";
                            }
                            else {
                                document.getElementById(progressId).style.display = "none";
                                document.getElementById("indexButton" + i).style.display = "block";
                            }
                        }
                    },
                    error: function(msg){
                        editRows[i]["progress"] = -1;
                        if (document.getElementById(progressId)) {
                            document.getElementById(progressId).style.display = "none";
                        }
                        if (document.getElementById("indexState" + i)) {
                            document.getElementById("indexButton" + i).style.display = "block";
                            document.getElementById("indexState" + i).style.display = "block";
                        }
                    }
                });
            }
        }
        timeoutId = setTimeout(function(){
            var flag = 0;
            for (var i=0; i<editRows.length; i++){
                progress = editRows[i]["progress"];
                progressId = "indexProgress" + i;
                if (parseFloat(progress, 10) >= 0 && parseFloat(progress, 10) < 100.0) {
                    flag = 1;
                }
                else if (document.getElementById(progressId)) {
                    document.getElementById(progressId).style.display = "none";
                    document.getElementById("indexButton" + i).style.display = "block";
                }
                if(parseInt(progress,10) < 0) {
                    if (document.getElementById("indexState" + i)) {
                        document.getElementById("indexState" + i).style.display = "block";
                    }
                }
            }
            if (flag == 1) {
                indexGetStatus(editRows);
            }
        }, 2000);
    }

    function indexQuery() {
        var indexname = $("#indexQueryName").val().trim();
        if(!indexname){
            indexInit();
        }
        else{
            $.ajax({
                cache: false,
                type: "GET",
                url: "/otscfgsvr/api/index/" + args_tablename + "/" + indexname + "?query_from=0",
                dataType: "json",
                timeout: 20000,
                success: function(results, msg){
                    if (results["errcode"] == 0) {
                        table_detail_indexes = [];
                        table_detail_indexes.push(results);
                        $('#tableDetailIndex').bootstrapTable('destroy').bootstrapTable({
                            data: table_detail_indexes,
                            classes: 'table',
                        });
                    }
                    else {
                        recordAlertMsg("查询索引信息" + errorInfo(results["errcode"]) );
                    }

                },
                error: function(msg){
                    var errmsg = "查询索引 " + getStructName(indexname) + " 信息失败！错误: " + getStructMsg(msg);
                    recordAlertMsg(errmsg);
                }
            });
        }
    }

    function clickIndexEdit(editRow, rowIndex) {
        modal_table_name = args_tablename;
        modal_index_name = editRow["index_name"];
        modal_index_info = editRow;
        table_detail_row_index = rowIndex;
        $('#indexEdit').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#indexEdit").modal({
            backdrop: "static",
            show: false,
            remote: "index_edit.jsp"
        });
    }

    function clickClearIndex(editRow, rowIndex) {
        var param = {"row":editRow, "index": rowIndex};
        var msg = '确定清空索引 ' + getStructName(editRow["index_name"]) + ' 的记录？';
        indexConfirmAlert(msg, 2, param);
    }

    function indexClear(editRow, rowIndex) {
        $.ajax({
            type:"PUT",
            url:"/otscfgsvr/api/index/" + args_tablename + "/"+ editRow["index_name"],
            data: "{\"truncate\":true}",
            dataType: "json",
            contentType: "application/json",
            timeout:30000,
            success:function(results, msg) {
                if (results.errcode != 0)
                {
                    recordAlertMsg("清空索引" + editRow["index_name"] + errorInfo(results["errcode"]));
                }
                else
                {
                    recordAlertMsg("清空索引" + editRow["index_name"] + " 成功！");
                    $.ajax({
                        cache: false,
                        type:"GET",
                        url:"/otscfgsvr/api/index/" + args_tablename + "/" + editRow["index_name"] + "?query_from=0",
                        dataType: "json",
                        timeout:10000,
                        success:function(results, msg) {
                            if(results["errcode"]==0){
                                var $table = $("#tableDetailIndex").bootstrapTable();
                                $table.bootstrapTable('updateRow', {
                                    index: rowIndex,
                                    row: {
                                        last_modify: results['last_modify'],
                                    }
                                });
                            }else{
                                recordAlertMsg(results["errinfo"]);
                            }

                        },
                        error: function(msg){
                            var errmsg = "获取索引 " + getStructName(editRow["index_name"]) + " 信息错误: " + getStructMsg(msg);
                            recordAlertMsg(errmsg);
                        }
                    });
                }
            },
            error: function(msg){
                var errmsg = "清空索引 " + getStructName(editRow["index_name"]) + " 失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function clickDeleteIndex(editRow) {
        var msg = '确定删除索引 ' + editRow["index_name"] + '？';
        indexConfirmAlert(msg, 0, editRow);
    }

    function indexDelete(editRow) {
        document.body.style.cursor = "wait";
        $.ajax({
            type:"DELETE",
            url:"/otscfgsvr/api/index/" + args_tablename + "/" + editRow["index_name"],
            timeout:100000,
            success:function(results, msg){
                if(results["errcode"] == 0){
                    var dataArray = [];
                    var valuesArray = [];
                    dataArray[0] = editRow;
                    valuesArray[0] = editRow["index_name"];
                    $table = $('#tableDetailIndex').bootstrapTable({
                        data: dataArray
                    });
                    $table.bootstrapTable('remove', {
                        field: 'index_name',
                        values: valuesArray
                    });
                    recordAlertMsg("删除索引" + editRow["index_name"] + " 任务提交成功！");
                } else{
                    recordAlertMsg("删除索引" + editRow["index_name"] + " 任务提交"  + errorInfo(results["errcode"]));
                }

            },
            complete : function() {
                document.body.style.cursor = "default";
            },
            error: function(msg){
                var errmsg = "删除索引 " + getStructName(editRow["index_name"]) + " 任务提交失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function clickDeleteMultiIndex() {
        var $table = $('#tableDetailIndex').bootstrapTable();
        var selects = $table.bootstrapTable('getSelections');
        if (selects.length == 0){
            recordAlertMsg("请选中至少一条索引。");
            return;
        }
        var msg = '确定删除选定的索引？';
        indexConfirmAlert(msg, 1, selects);
    }

    function indexMultiDelete(selects){
        $.map(selects, function (row) {
            var indexname = row["index_name"];
            document.body.style.cursor = "wait";
            $.ajax({
                type:"DELETE",
                url:"/otscfgsvr/api/index/" + args_tablename + "/" + indexname,
                timeout:100000,
                success:function(results, msg){
                    if (results.errcode != 0) {
                        recordAlertMsg("重建索引" + indexname + "任务提交" + errorInfo( results["errcode"]));
                    }else{
                        var dataArray = [];
                        var valuesArray = [];
                        dataArray[0] = row;
                        valuesArray[0] = indexname;
                        $table = $('#tableDetailIndex').bootstrapTable({
                            data: dataArray
                        });
                        $table.bootstrapTable('remove', {
                            field: 'index_name',
                            values: valuesArray
                        });
                    }
                },
                complete : function() {
                    document.body.style.cursor = "default";
                },
                error: function(msg){
                    var errmsg = "删除索引 " + getStructName(indexname) + " 任务提交失败！错误: " + getStructMsg(msg);
                    recordAlertMsg(errmsg);
                }
            });
        });
    }

    function clickRebuildIndex(editRow, rowIndex) {
        var param = {"row":editRow, "index": rowIndex};
        var msg = '确定重建索引 ' + getStructName(editRow["index_name"]) + '？';
        indexConfirmAlert(msg, 3, param);
    }

    function indexRebuild(editRow, rowIndex){
        var indexname = editRow["index_name"];
        $.ajax({
            type:"PUT",
            url:"/otscfgsvr/api/index/" + args_tablename + "/"+ indexname,
            data:"{\"rebuild\":true}",
            dataType: "json",
            contentType: "application/json",
            timeout:30000,
            success:function(results, msg) {
                if (results.errcode != 0) {
                    recordAlertMsg("重建索引" + indexname + " 任务提交 " +errorInfo( results["errcode"]) );
                }
                else {
                    recordAlertMsg("重建索引" + indexname + " 任务提交成功！");
                    $.ajax({
                        cache: false,
                        type:"GET",
                        url:"/otscfgsvr/api/index/" + args_tablename + "/" + indexname + "?query_from=0",
                        dataType: "json",
                        timeout:10000,
                        success:function(results, msg) {
                            var $table = $("#tableDetailIndex").bootstrapTable();
                            $table.bootstrapTable('updateRow', {
                                index: rowIndex,
                                row: {
                                    last_modify: results['last_modify'],
                                }
                            });
                            editRow["progress"] = 0;
                        },
                        error: function(msg){
                            var errmsg = "获取索引 " + getStructName(indexname) + " 信息失败！错误: " + getStructMsg(msg);
                            recordAlertMsg(errmsg);
                        }
                    });
                }
            },
            error: function(msg){
                var errmsg = "重建索引" + getStructName(indexname) + " 任务提交失败！错误: " + getStructMsg(msg);
                recordAlertMsg(errmsg);
            }
        });
    }

    function goIndexView(indexname, idxtype, tableid){
        var url = "index_view.jsp?tablename=" + args_tablename + "&indexname=" + indexname + "&pktype=" + args_pktype +"&id=" + tableid;
        if (args_pktype == 1) {
            url += "&range=" + args_range;
        }
        url += "&idxtype=" + idxtype;
        window.location=url;
    }

    function clickCreateIndex() {
        modal_table_name = args_tablename;
        $('#indexCreate').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#indexCreate").modal({
            backdrop: "static",
            show: false,
            remote: "index_create.jsp"
        });
    }

    function clickAddRecord() {
        modal_table_name = args_tablename;
        $('#recordCreate').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#recordCreate").modal({
            backdrop: "static",
            show: false,
            remote: "record_create.jsp"
        });
    }

    function clickEditDisplayColumns() {
        modal_table_name = args_tablename;
        $('#propertyEdit').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#propertyEdit").modal({
            backdrop: "static",
            show: false,
            remote: "property_edit.jsp"
        });
    }

    function clickEditRecord(editRow) {
        modal_table_name = args_tablename;
        modal_hash_key = htmlUnescape(editRow["hash_key"]);
        if (args_pktype == 1) {
            modal_range_key = htmlUnescape(editRow["range_key"]);
        }
        $('#recordEdit').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#recordEdit").modal({
            backdrop: "static",
            show: false,
            remote: "record_edit.jsp"
        });
    }

    function clickStrategyDelete() {
        modal_table_name = args_tablename;
        $('#strategyDelete').on("show.bs.modal", function () {
            $(this).removeData("bs.modal");
        });
        $("#strategyDelete").modal({
            backdrop: "static",
            show: false,
            remote: "record_delete.jsp"
        });
    }

    function textFocus(type){
        if (type == 0){
            $("#AccurateKeyRadio").attr("checked", true);
        }
        else if (type == 1){
            $("#rangeStartEndKeyRadio").attr("checked", true);
        }
    }

    function getTableDetailShowColumns() {
        var propertyShowNum = 2;
        if (args_pktype == 1) {
            propertyShowNum = 3;
        }
        for (var i=0; i<table_detail_query_columns.length; i++) {
            var columnMap = {"align":"center"};
            columnMap["field"] = table_detail_query_columns[i];
            columnMap["title"] = htmlEscape(table_detail_query_columns[i]);
            table_detail_show_columns[propertyShowNum] = columnMap;
            propertyShowNum++;
        }
        for (var i=propertyShowNum; i<table_detail_show_columns_num; i++) {
            table_detail_show_columns[i] = table_detail_column_property;
        }
    }
</script>
</html>
