<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<div class="modal-header" style="background-color:#f5f5f5;">
	<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
    <h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">表授权</h4>
</div>

<div class="modal-body" style="width:100%"> 

	<div class="row" style="margin-top:15px;">
	  <table>
		<tr>
			<td ><div style="margin-left:5px;height:25px">群组名：</div></td>
			<td ><input id="groupList" type='text' style="text-align:left;width:100px;height:25px;"/> </td>
			<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>
			<td ><div style="text-align:left;width:40px;height:25px;">授权:</div></td>
			<td ><input id="read_authority" type="checkbox"> </td>
			 <td>&nbsp;</td>
			<td ><div style="text-align:left;width:40px;height:25px;">读</div></td>
			<td ><input id="mangement_authority" type="checkbox"></td>
			<td>&nbsp;</td>
			<td ><div style="text-align:left;width:40px;height:25px;">管理</div></td>
			<td> <span id="addpermission" class="glyphicon glyphicon-plus" style="font-size:18px;" title="增加" onclick='addGroupAuthority();'></span></td>
		</tr>
	  </table>
	</div>
	<div class="row" style="margin-top:15px;">
		<table id="tagInfo" style="table-layout:fixed; word-wrap:break-word;border-collapse:collapse;data-height:300px;max-height:400px;overflow-y:scroll">
      		<thead>
      			<tr>      			
      				<th data-field="name" data-align="center" data-searchable="false">群组名称</th>
      				<th data-field="READ" data-align="center" data-searchable="false">读取权限</th>
					<th data-field="MANAGE" data-align="center"  data-searchable="false">管理权限</th>
        			<th class="col-desc-30" data-field="desc" data-align="center" data-formatter="permissionDelete" data-events="operateEvents">操作</th>
      			</tr>
      		</thead> 
		</table>
	</div> 
    	
    <div class="row" style="width:100%;margin-left:10px;margin-top:0px;height:20px;align:center">
    	<div class="r_text14" id="selectStatus" style="word-wrap:break-word;border-collapse:collapse;margin-left:10px;width:90%;text-align:center"></div>
	</div> 	
	<div  class="row"></div>
</div> 

<script type="text/javascript">	
    	//debugger;
	var query_condition = {	'page' : 1 };
	$(function() {
	    var resultFinal = '没有相关记录';
		$("#groupList").html("");
		$("#selectStatus").html("");
		$("#selectTable").css("min-height", "500px");
		$("#prop_type").html("");

		$("#mangement_authority").on("click", function() {
			if ($("#mangement_authority").attr("checked")) {
				$("#read_authority").prop("checked", true);
			}
		});
		$("#read_authority").on("click", function() {
			if (!$("#read_authority").attr("checked")) {
				$("#mangement_authority").prop("checked", false);
			}
		});
		if (propType > 0) {
			$("#prop_type").append(typeFormatter(propType));
		} else {
			$("#prop_type").append("-");
		}
		getGroupNameList();
		$("input[type='text']").focus(function() {
			$("#selectStatus").html("");
		});

		$("#tagInfo").bootstrapTable('destroy');
		$('#tagInfo').bootstrapTable(
		{
			url : '/otscfgsvr/servlet/group_authority_list',
			classes : 'table',
			striped : true,
			pagination : true,
			sidePagination : 'server',
			pageSize : 10,
			pageNumber : 1,
			pageList : [ 5, 10, 15 ],
			queryParamsType : 'limit',
			//此处Bootstrap插件为我们将查询的参数和占位符“？”自动拼接在url里面
			queryParams : function(params) {
				params["tableId"] = table_id;
				params["page"] = params["offset"] / params["limit"] + 1;
				return params;
			},
			responseHandler : function(res) {
			    if(res["errcode"] == 176602){
                    resultFinal = '查询记录异常，请联系管理员';
				}
				return {
					rows : res.groups,
					total : res.total_count
				};
			},
			formatRecordsPerPage : function(pageNumber) {
				return sprintf('每页显示 %s 条记录', pageNumber);
			},
			formatShowingRows : function(pageFrom, pageTo, totalRows) {
				return sprintf('总共 %s 条记录&nbsp;&nbsp;第  %s 到 %s 条记录', totalRows, pageFrom, pageTo);
			},
			formatNoMatches : function() {
				return resultFinal;
			},
		});
	});

	function getGroupNameList() {
		$.ajax({
			type : "GET",
			url : "/otscfgsvr/servlet/group_list",
			dataType : "json",
			timeout : 30000,
			success : function(results, msg) {
				try {
					var errcode = results["errcode"];
					if (errcode == 0) {
						var len = results["total_count"];
						if (len != null) {
							for (var i = 0; i < len; i++) {
								all_group_lists[i] = results["groups"][i].name;
							}
							all_group_lists.splice($.inArray("admingroup", all_group_lists), 1);
							$("#groupList").autocomplete({
								minLength : 0,
								source : all_group_lists.sort(),
								focus : function(event, ui) {
									$("#groupList").val(ui.item.label);
									return false;
								},
								select : function(event, ui) {
									$("#groupList").val(ui.item.label);
									return false;
								},
							}).click(function() {
								$(this).autocomplete('search', '');
							});
						}
					} else {
						$("#selectStatus").html("");
						$("#selectStatus").append(errorInfo(results["errcode"]));
						$("#groupList").val("");
					}
				} catch (error) {
					console.log("Failed to otscfgsvr obtain grouplist, reason as: " + error);
				}
			},
			complete : function() {
				document.body.style.cursor = "default";
			},
			error : function(msg) {
				var errmsg = "获取群组名列表错误: " + getStructMsg(msg);
				tableAlertMsg(errmsg);
			}
		});
	}

	function addGroupAuthority() {
		var mapValue = {};
		mapValue["relevantService"] = "OTS";
		mapValue["tableId"] = table_id; //在table_list.jsp中定义
		mapValue["display_resourceName"] = resource_disp;
		if ($("#resource_desc").length == 0 || resource_desc == null) {
			mapValue["description_resourceName"] = resource_disp;
		} else {
			mapValue["description_resourceName"] = resource_desc;
		}
		var notequal = false;
		if ($("#groupList").val() == "无权限操作") {
			return;
		}
		if ($("#groupList").val() != "") {
			for (var j = 0; j < all_group_lists.length; j++) {
				if ($("#groupList").val() === all_group_lists[j]) {
					notequal = true;
					mapValue["group_name"] = $("#groupList").val();
					break;
				}
			}
			if (notequal == true) {
				mapValue["relevant_authority"] = {};
				if ($("#mangement_authority").prop("checked")) {
					mapValue["relevant_authority"]["GET"] = true;
					mapValue["relevant_authority"]["POST"] = true;
					mapValue["relevant_authority"]["PUT"] = true;
					mapValue["relevant_authority"]["DELETE"] = true;
				} else {
					if ($("#read_authority").prop("checked")) {
						mapValue["relevant_authority"]["GET"] = true;
					} else {
						mapValue["relevant_authority"]["GET"] = false;
					}
					mapValue["relevant_authority"]["POST"] = false;
					mapValue["relevant_authority"]["PUT"] = false;
					mapValue["relevant_authority"]["DELETE"] = false;
				}

				$.ajax({
					async : false,
					type : "POST",
					url : "/otscfgsvr/servlet/add_authority",
					data : JSON.stringify(mapValue), /*将map数据转化为Json数据，然后将这些参数传给后台（parame）*/
					dataType : "json",
					timeout : 30000,
					success : function(results, msg) { /*results是后台返回的参数*/
						try {
							$("#selectStatus").html("");
							$("#selectStatus").append(errorInfo(results["errcode"]));
							$("#groupList").val("");
						} catch (error) {
							//alert( error);
						}
					},
					complete : function() {
						document.body.style.cursor = "default";
					},
					error : function(msg) {
						var errmsg = "删除实例 " + " 失败。";
						tableAlertMsg(errmsg);
					}
				});
				$("#tagInfo").bootstrapTable('selectPage', 1);
				$('#tagInfo').bootstrapTable('refresh', null);
			} else {
				$("#selectStatus").html("");
				$("#selectStatus").append("已有群组中不包含输入项，请检查后重新输入！");
			}
		} else {
			$("#selectStatus").html("");
			$("#selectStatus").append("群组名不能为空！");
		}
	}

	function permissionDelete(value, row, index) {
		return [ '<input id="btn_canceltag" name = "" type ="button" class = "deletePermission btn3" style = "width:75px;" value= "删除">' ]
				.join('');
	}
</script>