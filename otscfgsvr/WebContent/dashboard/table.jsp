<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page language="java" import="com.baosight.xinsight.ots.cfgsvr.bean.AuthBean" %>

<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8"/>
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>表的管理--OTS 配置中心</title>
    <link rel="shortcut icon" href="jsp/images/icon.ico"/>
    <link href="jsp/bootstrap-3.2.0-dist/css/bootstrap.min.css" rel="stylesheet" type="text/css">
    <link href="jsp/css/bootstrap-table.css" rel="stylesheet" type="text/css">
    <link href="jsp/css/style.css" rel="stylesheet" type="text/css">
    <link href="jsp/jquery-ui-1.11.4.custom/jquery-ui.min.css" rel="stylesheet" type="text/css">
    <link rel="prefetch prerender" href="help.jsp"/>
    <link rel="prefetch prerender" href="metric.jsp"/>
    <style type="text/css">
        .ui-autocomplete {
            position: absolute;
            top: 0;
            left: 0;
            cursor: default;
            z-index: 100000;
        }
    </style>

    <script type="text/javascript" src="jsp/js/jquery-1.8.2.min.js"></script>
    <script type="text/javascript" src="jsp/js/jquery.validate.min.js"></script>
    <script type="text/javascript" src="jsp/js/messages_zh.js"></script>
    <script type="text/javascript" src="jsp/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="jsp/js/bootstrap-table.js"></script>
    <script type="text/javascript" src="jsp/js/init-js.js"></script>
    <script type="text/javascript" src="jsp/js/exceptionDic.js"></script>
    <script type="text/javascript" src="jsp/jquery-ui-1.11.4.custom/jquery-ui.min.js"></script>
    <script type="text/javascript" src="jsp/js/common.js"></script>

    <script type="text/javascript">
        $(function () {
                showActivePage("table");
            }
        );
    </script>
</head>

<body class="body_fix">

<div id="header">
    <jsp:include page="header.jsp"/>
</div>
<div class="content">
    <!---content--->
    <div class="row">
        <div class="row" style="height:40px;">
            <div class="pageicon">|</div>
            <div style="margin-left:5px;margin-top:5px;">
                <ul class="breadcrumb pagetitle" style="background-color:transparent;padding-top:0px;padding-left:5px;">
                    <li><a href="table.jsp">OTS-表的管理</a></li>
                </ul>
            </div>
        </div>
        <div class="row" style="margin-left:70px;margin-top:20px;">
            <span>表名：</span>
            <input name="" id="tableName" type="text" class="textbox1" style="width:200px;height:25px;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <input name="" type="button" class="btn2" style="width:50px" value="查询" onclick='tableQuery()'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
        </div>
        <div class="row" style="margin-left:70px;margin-right:70px;margin-top:15px;">
            <% String token = session.getAttribute("token").toString(); %>
            <% String tenant = session.getAttribute("tenant").toString(); %>
            <% String username = session.getAttribute("username").toString(); %>
            <% String tenantId = session.getAttribute("tenantId").toString(); %>
            <% String userId = session.getAttribute("userId").toString(); %>
            <jsp:useBean id="test" class="com.baosight.xinsight.ots.cfgsvr.bean.AuthBean"/>
            <jsp:setProperty name="test" property="token" value="<%=token %>"/>
            <jsp:setProperty name="test" property="username" value="<%=username %>"/>
            <jsp:setProperty name="test" property="tenant" value="<%=tenant %>"/>
            <jsp:setProperty name="test" property="tenantId" value="<%=tenantId %>"/>
            <jsp:setProperty name="test" property="userId" value="<%=userId %>"/>
            <jsp:setProperty name="test" property="serviceName" value="OTS"/>
            <c:if test="${test.hasManagePerm }">
                <div class="table-controls">
                    <div class="pull-right">
                        <input name="" type="button" class="btn" style="width: 70px;" value="新建表"
                               onclick="clickCreateTable();" data-toggle="modal" data-target="#tableCreate">
                        <input name="" type="button" class="btn" style="width: 80px;" value="批量删除"
                               onclick="clickDeleteMultiTables();" data-method="remove">
                        <input name="" type="button" class="btn" style="width: 70px;" value="恢复表"
                               onclick="clickRestoreTable();" data-toggle="modal" data-target="#tableRestore">
                    </div>
                </div>
            </c:if>
            <table id="tableManage" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse">
                <thead>
                <tr>
                    <th data-field="state" data-checkbox="true"></th>
                    <th data-field="table_name" data-align="center" data-formatter="nameFormatter">表名</th>
                    <th data-field="primary_key_type" data-align="center" data-formatter="primaryKeyTypeFormatter">主键类型</th>
                    <th data-field="compression_type" data-align="center" data-formatter="compressionFormatter">压缩算法</th>
                    <!-- <th data-field="mob_enabled" data-align="center" data-formatter="mobFormatter">是否支持Mob</th> -->
                    <!-- <th data-field="mob_threshold" data-align="center">阈值（KB）</th> -->
                    <th data-field="description" data-align="center" data-formatter="descFormatter">描述</th>
                    <th class="col-operate-operation" data-field="operate" data-align="center"
                        data-formatter="operateFormatter" data-events="operateEvents">操作</th>
                </tr>
                </thead>
            </table>
        </div>
    </div>
</div>
<div>
    <jsp:include page="footer.jsp"/>
</div>
<!---dialog: table--->
<div class="modal text-center" id="tableCreate" tabindex="-1" role="dialog" aria-labelledby="myModalLabel"
     aria-hidden="true">
    <div class="modal-dialog" style="display: inline-block; width: auto;">
        <div class="modal-content"></div>
    </div>
</div>
<div class="modal text-center" id="tableUpdate" tabindex="-1" role="dialog" aria-labelledby="myModalLabel"
     aria-hidden="true">
    <div class="modal-dialog" style="display: inline-block; width: auto;">
        <div class="modal-content"></div>
    </div><!-- /.modal -->
</div>
<div class="modal text-center" id="tableBackup" tabindex="-1" role="dialog" aria-labelledby="myModalLabel"
     aria-hidden="true">
    <div class="modal-dialog" style="display: inline-block; width: auto;">
        <div class="modal-content"></div>
    </div><!-- /.modal -->
</div>
<div class="modal text-center" id="tableRestore" tabindex="-1" role="dialog" aria-labelledby="myModalLabel"
     aria-hidden="true">
    <div class="modal-dialog" style="display: inline-block; width: auto;">
        <div class="modal-content"></div>
    </div><!-- /.modal -->
</div>
<div>
    <div class="modal text-center" id="tablePermission" tabindex="-1" role="dialog" aria-labelledby="myModalLabel"
         aria-hidden="true">
        <div class="modal-dialog" style="display: inline-block; width: 650px;">
            <div class="modal-content"></div>
        </div><!-- /.modal -->
    </div>
    <div>
        <jsp:include page="errorTips.jsp"/>
    </div>
</body>

<script type="text/javascript">
    var all_table_info_;
    var all_table_info;
    var table_row_index = 0;
    var modal_table_row = [];
    var all_group_lists = [];
    var table_id = "";
    var resource_name = "";
    var propType = "";
    var resource_desc = "";
    var resource_disp = "";

    $(function () {
        $('#tableManage').bootstrapTable({
            data: [],
            classes: 'table',
            striped: true,
            pagination: true,
            pageSize: 5,
            pageList: [5, 10, 15],
            formatRecordsPerPage: function (pageNumber) {
                return sprintf('每页显示 %s 条记录', pageNumber);
            },
            formatShowingRows: function (pageFrom, pageTo, totalRows) {
                return sprintf('第  %s 到 %s 条记录', pageFrom, pageTo);
            },
            formatNoMatches: function () {
                return '';
            },
        });
        /* $('#tableManage').on('pre-body.bs.table', function (data, rows) {
            getBackupStatus(rows);
         }); */
        getTableInfo();
    });

    function getTableInfo() {
        $.ajax({
            cache: false,
            type: "GET",
            url: "/otscfgsvr/api/table/_all_tables_info",
            dataType: "json",
            timeout: 30000,
            success: function (results, msg) {
                if (results["errcode"] == 0) {
                    all_table_info_ = results["table_info_list"];
                    getBackupStatus();
                }
                else {
                    tableAlertMsg("获取表的列表失败！错误: " + results["errcode"]);
                }
            },
            complete: function () {
                var $table = $('#tableManage').bootstrapTable();
                var pagesize = $table.bootstrapTable('getOptions').pageSize;
                $('#tableManage').bootstrapTable('destroy').bootstrapTable({
                    data: all_table_info_,
                    classes: 'table',
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
            error: function (msg) {
                var errmsg = "获取表的列表失败！错误: " + getStructMsg(msg);
                tableAlertMsg(errmsg);
            }
        });
    }


    function nameFormatter(value, row) {
        return '<a style="cursor:pointer;" onclick=\'goTableDetail("' + row["table_name"] + '","' + row["primary_key_type"] + '","' + row["range_key_type"] + '","' + row["id"] + '");\' title=' + value + '>' + getStructName(value) + '</a>';
    }

    function compressionFormatter(value) {
        switch (value) {
            case 0:
                return "NONE";
            case 1:
                return "SNAPPY";
            case 2:
                return "LZ4";
            case 3:
                return "GZ";
            case 4:
                return "LZO";
            default:
                return "UNDEFIEND";
        }
    }

    function primaryKeyTypeFormatter(value) {
        switch (value) {
            case 0:
                return "H";
            case 1:
                return "H + R";
            default:
                return "UNDEFIEND";
        }
    }

    function mobFormatter(value) {
        if (value == true) {
            return '是';
        }
        else {
            return '否';
        }
    }

    function descFormatter(value) {
        return '<span title="' + value + '">' + htmlEscape(getStructName(value)) + '</span>';
    }

    function operateFormatter(value, row, index) {
        var operate = [
            '<div class="progress progress-striped" id="tableProgress' + index + '" style="margin-bottom:5px;display:none"><div class="progress-bar" id="tableProgressBar' + index + '" role="progressbar" style="width:' + row["progress"] + '">',
            '<span class="progress_text">正在进行备份/恢复……</div></div>',
            '<div id="tableButton' + index + '" style="display:block">',
            '<input name="" type="button" class="edit btn3" style="width:40px;" value="编辑" data-toggle="modal" data-target="#tableUpdate">',
            '&nbsp;&nbsp;',
            ' <c:if test="${test.hasManagePerm }">',
            '<input name="" type="button" class="delete btn3" style="width:40px;" value="删除" data-method="remove">',
            '&nbsp;&nbsp;',
            ' </c:if >',
            '<input name="" type="button" class="backup btn3" style="width:40px;" value="备份" data-toggle="modal" data-target="#tableBackup">',
            '&nbsp;&nbsp;',
            ' <c:if test="${test.hasAuthPerm }">',
            '<input name="" type="button" class="permit btn3" style="width:40px;" value="授权"  data-toggle="modal" data-target="#tablePermission">',
            ' </c:if >',
            '&nbsp;&nbsp;',
            '<div id="tableState' + index + '" class="r_text10" style="float:right;'
        ];

        if (!row["result"] || row["result"] == 0) {
            operate.push('display:none;');
        }
        operate.push('">备份/恢复失败！</div>');

        return operate.join('');
    }

    window.operateEvents = {
        'click .edit': function (e, value, row, index) {
            clickEditTable(row, index);
        },
        'click .delete': function (e, value, row, index) {
            clickDeleteTable(row);
        },
        'click .backup': function (e, value, row, index) {
            clickBackupTable(row, index);
        },
        'click .permit': function (e, value, row, index) {
            clickPermitTable(row);
        },
        'click .deletePermission': function (e, value, row, index) {
            deletePermission(row, index);
        }

    };

    function tableAlertMsg(msg) {
        errorAlertMsg(msg);
    }

    function tableConfirmAlert(msg, operate, param) {
        confirmAlertMsg(msg, 0, operate, param);
    }

    function tableQuery() {
        var tablename = $("#tableName").val().trim();

        if (!tablename) {
            all_table_info_ = [];
            getTableInfo();
            return;
        }
        else {
            $.ajax({
                cache: false,
                type: "GET",
                url: "/otscfgsvr/api/table/" + encodeURIComponent(tablename),
                dataType: "json",
                timeout: 30000,
                success: function (results, msg) {
                    if (results["errcode"] == 0) {
                        all_table_info_ = [];
                        all_table_info_[0] = results;//必须视为数组,才能确保edit页面正确
                        $('#tableManage').bootstrapTable('destroy').bootstrapTable({
                            data: all_table_info_,
                            classes: 'table',
                        });
                    }
                    else {
                        tableAlertMsg("获取表 " + tablename + " 信息失败！错误: " + results["errcode"]);
                    }
                },
                error: function (msg) {
                    var errmsg = "获取表 " + getStructName(tablename) + " 信息失败！错误: " + getStructMsg(msg);
                    tableAlertMsg(errmsg);
                }
            });
        }
    }

    function clickDeleteMultiTables() {
        var $table = $('#tableManage').bootstrapTable();
        var selects = $table.bootstrapTable('getSelections');
        if (selects.length == 0) {
            tableAlertMsg("请选中至少一张表。");
            return;
        }
        var msg = '确定删除选定的表（同时将删除包含的索引）？';
        tableConfirmAlert(msg, 1, selects);
    }

    function tableMultiDelete(selects) {
        $.map(selects, function (row) {
            var tablename = row["table_name"];
            document.body.style.cursor = "wait";
            $.ajax({
                type: "DELETE",
                url: "/otscfgsvr/api/table/" + tablename,
                timeout: 100000,
                success: function (results, msg) {
                    if (results["errcode"] != 0) {
                        tableAlertMsg("删除表 " + errorInfo(results["errcode"]));
                        return;
                    }
                    var dataArray = [];
                    var valuesArray = [];
                    dataArray[0] = row;
                    valuesArray[0] = tablename;
                    $table = $('#tableManage').bootstrapTable({
                        data: dataArray
                    });
                    $table.bootstrapTable('remove', {
                        field: 'table_name',
                        values: valuesArray
                    });
                },
                complete: function () {
                    document.body.style.cursor = "default";
                },
                error: function (msg) {
                    var errmsg = "删除表 " + getStructName(tablename) + "失败！ 错误: " + getStructMsg(msg);
                    tableAlertMsg(errmsg);
                }
            });
        });
    }

    function clickDeleteTable(editRow) {
        var tablename = editRow["table_name"];
        var msg = '确定删除表 ' + tablename + '（同时将删除包含的索引）？';
        tableConfirmAlert(msg, 0, editRow);
    }

    function tableDelete(editRow) {
        document.body.style.cursor = "wait";
        $.ajax({
            type: "DELETE",
            url: "/otscfgsvr/api/table/" + editRow["table_name"],
            timeout: 100000,
            success: function (results, msg) {
                if (results["errcode"] != 0) {
                    tableAlertMsg("删除表 " + errorInfo(results["errcode"]));
                    return;
                }
                var dataArray = [];
                var valuesArray = [];
                dataArray[0] = editRow;
                valuesArray[0] = editRow["table_name"];
                $table = $('#tableManage').bootstrapTable({
                    data: dataArray,
                });
                $table.bootstrapTable('remove', {
                    field: 'table_name',
                    values: valuesArray,
                });
                tableAlertMsg("删除表 " + editRow["table_name"] + " 成功！");
            },
            complete: function () {
                document.body.style.cursor = "default";
            },
            error: function (msg) {
                var errmsg = "删除表 " + getStructName(editRow["table_name"]) + " 失败！错误: " + getStructMsg(msg);
                tableAlertMsg(errmsg);
            }
        });
    }

    function deletePermission(cancel_row, cancel_index) {
        var mapCancel = {}; //存储模态弹窗的
        mapCancel["group_name"] = cancel_row["name"].trim();
        mapCancel["tableId"] = table_id;
        $("#selectStatus").html("");
        if (window.confirm("确定删除群组关联资源 " + cancel_row["name"] + "?")) {
            $.ajax({
                type: "DELETE",
                url: "/otscfgsvr/servlet/delete_authority",
                data: JSON.stringify(mapCancel),
                dataType: "json",
                timeout: 30000,
                success: function (results, msg) {
                    try {
                        $('#tagInfo').bootstrapTable('refresh', null);
                        $("#selectStatus").html("");
                        $("#selectStatus").append("删除").append(errorInfo(results["errcode"]));
                        $("#cnamelist").val("");
                        return;
                    } catch (error) {
                        //	alert( error);
                    }
                },
                complete: function () {
                    document.body.style.cursor = "default";
                },
                error: function (msg) {
                    var errmsg = "后台删除错误: " + getStructMsg(msg);
                    tableAlertMsg(errmsg);
                }
            });
        } else {
            return false;
        }
    }

    $('#tableCreate').on("show.bs.modal", function () {
        $(this).removeData("bs.modal");
    });

    function clickCreateTable() {
        $("#tableCreate").modal({
            backdrop: "static",
            show: false,
            remote: "table_create.jsp"
        });
    }

    $('#tableUpdate').on("show.bs.modal", function () {
        $(this).removeData("bs.modal");
    });

    function clickEditTable(editRow, rowIndex) {
        modal_table_name = editRow["table_name"];
        table_row_index = rowIndex;
        all_table_info = all_table_info_;
        $("#tableUpdate").modal({
            backdrop: "static",
            show: false,
            remote: "table_edit.jsp"
        });
    }

    $('#tableBackup').on("show.bs.modal", function () {
        $(this).removeData("bs.modal");
    });

    function clickBackupTable(editRow, rowIndex) {
        modal_table_name = editRow["table_name"];
        table_row_index = rowIndex;
        modal_table_row = editRow;
        $("#tableBackup").modal({
            backdrop: "static",
            show: false,
            remote: "table_backup.jsp"
        });
    }

    $('#tableRestore').on("show.bs.modal", function () {
        $(this).removeData("bs.modal");
    });

    function clickRestoreTable() {
        all_table_info = all_table_info_;
        $("#tableRestore").modal({
            backdrop: "static",
            show: false,
            remote: "table_restore.jsp"
        });
    }

    $('#tablePermission').on("show.bs.modal", function () {
        $(this).removeData("bs.modal");
    });

    function clickPermitTable(row_param) {
        propType = row_param["prop_type"];
        table_id = row_param["id"];
        resource_desc = row_param["description"];
        resource_disp = row_param["table_name"];
        $("#tablePermission").modal({
            backdrop: "static",
            show: false,
            remote: "add_permit_object.jsp",
        });
    }

    function getBackupStatus() {
        $.ajax({
            cache: false,
            async: false,
            type: "GET",
            url: "/otscfgsvr/api/table/status/_all_tables",
            dataType: "json",
            timeout: 20000,
            success: function (results, msg) {
                if (results["errcode"] == 0) {
                    var table_status_list = results["table_status_list"];
                    for (var i = 0; i < table_status_list.length; i++) {
                        for (var j = 0; j < all_table_info_.length; j++) {
                            if (table_status_list[i]["table_name"] == all_table_info_[j]["table_name"]) {
                                all_table_info_[j]["progress"] = table_status_list[i]["progress"];
                                all_table_info_[j]["result"] = table_status_list[i]["result"];
                                all_table_info_[j]["backup_state"] = table_status_list[i]["state"];
                                all_table_info_[j]["tenant_state"] = table_status_list[i]["tenant_state"];
                                if (parseInt(table_status_list[i]["state"], 10) == 0 || parseInt(table_status_list[i]["state"], 10) == 1) {
                                    getOneTableBackupStatus(all_table_info_[j], j);
                                } else {
                                    if (table_status_list[i]["result"] != null
                                        && table_status_list[i]["result"] != 0) {
                                        if (document.getElementById("tableState" + j)) {
                                            document.getElementById("tableState" + j).style.display = "block";
                                        }
                                    } else {
                                        if (document.getElementById("tableState" + j)) {
                                            document.getElementById("tableState" + j).style.display = "none";
                                        }
                                    }
                                    var progress = all_table_info_[j]["progress"];
                                    var progressId = "tableProgress" + j;
                                    if (document.getElementById(progressId)) {
                                        if (parseFloat(progress, 10) >= 0
                                            && parseFloat(progress, 10) < 100.0) {
                                            document.getElementById("tableButton" + i).style.display = "none";
                                            document.getElementById(progressId).style.display = "block";
                                            document.getElementById("tableProgressBar" + j).style.width = all_table_info_[i]["progress"] + "%";
                                        } else {
                                            document.getElementById(progressId).style.display = "none";
                                            document.getElementById("tableButton" + j).style.display = "block";
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            },
            error: function (msg) {
                var errmsg = "获取表 "
                    + getStructName(tablename)
                    + " 备份/恢复状态信息失败！错误: "
                    + getStructMsg(msg);
                tableAlertMsg(errmsg);
            }
        });
    }


    function getOneTableBackupStatus(editRow, rowIndex) {
        var progressId = "tableProgress" + rowIndex;
        var progressBarId = "tableProgressBar" + rowIndex;
        var progress = editRow["progress"];
        $.ajax({
            cache: false,
            async: false,
            type: "GET",
            url: "/otscfgsvr/api/table/status/" + editRow["table_name"],
            dataType: "json",
            timeout: 20000,
            success: function (results, msg) {
                editRow["progress"] = results["progress"];
                editRow["result"] = results["result"];
                editRow["backup_state"] = results["state"];
                editRow["tenant_state"] = results["tenant_state"];
                progress = editRow["progress"];
                if (document.getElementById(progressId)) {
                    if (editRow["result"] != null && editRow["result"] != 0) {
                        document.getElementById(progressId).style.display = "none";
                        document.getElementById("tableButton" + rowIndex).style.display = "block";
                        if (document.getElementById("tableState" + rowIndex)) {
                            document.getElementById("tableState" + rowIndex).style.display = "block";
                        }
                    } else {
                        if (document
                                .getElementById("tableState" + rowIndex)) {
                            document.getElementById("tableState" + rowIndex).style.display = "none";
                        }
                        if (parseFloat(progress, 10) >= 0 && parseFloat(progress, 10) < 100.0) {
                            document.getElementById("tableButton" + rowIndex).style.display = "none";
                            document.getElementById(progressId).style.display = "block";
                            document.getElementById(progressBarId).style.width = editRow["progress"] + "%";
                        } else {
                            document.getElementById(progressId).style.display = "none";
                            document.getElementById("tableButton" + rowIndex).style.display = "block";
                        }
                    }
                }
            },
            error: function (msg) {
                if (document.getElementById(progressId)) {
                    document.getElementById(progressId).style.display = "none";
                }
                if (document.getElementById("tableState" + rowIndex)) {
                    document.getElementById("tableButton" + rowIndex).style.display = "block";
                    document.getElementById("tableState" + rowIndex).style.display = "block";
                }
            }
        });
        setTimeout(function () {
            if (parseInt(editRow["backup_state"]) == 0 || parseInt(editRow["backup_state"]) == 1) {
                getOneTableBackupStatus(editRow, rowIndex);
            }
        }, 2000);
    }

    function goTableDetail(tablename, keytype, rangetype, id) {
        var url = "table_detail.jsp?tablename=" + tablename + "&pktype=" + keytype + "&id=" + id;
        if (keytype == 1 && rangetype != null) {
            url += "&range=" + rangetype;
        }
        window.location = url;
    }
</script>
</html>
