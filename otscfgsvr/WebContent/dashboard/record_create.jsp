<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%>
<div class="modal-header" style="background-color:#f5f5f5;">
	<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>
	<h1 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">添加记录</h1>
</div>
<div class="modal-body">
	<div class="row" style="text-align:left;margin-top:0px;">记录详细信息：
		<div class="btn-group pull-right btn-group-xs">
			<button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown" onclick='insertExample(1);' >使用示例 </button>
			<%--<button type="button" class="btn btn-default dropdown-toggle" data-toggle="dropdown"  >使用示例 <span class="caret"></span></button>--%>
			<%--<ul class="dropdown-menu" role="menu">--%>
			<%--<li><a class="btn-xs" href="#" onclick='insertExample(1);'>Hash示例</a></li>--%>
			<%--&lt;%&ndash;<li><a class="btn-xs" href="#" onclick='insertExample(2);'>Hash+Range示例</a></li>&ndash;%&gt;--%>
			<%--</ul>--%>
		</div>
	</div>
	<div class="row" style="height:35px;"><textarea name="" id="recordCreateContent" cols="" rows="" style="width:550px;height:210px;max-width:550px;max-height:210px;resize:none;"></textarea></div>
	<div class="row" style="margin-top:180px;height:20px;"><div class="r_text14" id="recordCreateStatus" style="margin-left:10px;width:550px;text-align:center"></div></div>
	<div class="row" style="width:550px;margin-top:0px;">
		<div class="cy_text">
			<input id="btn_createrecord" name="" type="button" class="btn2" style="width:60px" value="创建" onclick='recordCreate();'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
			<input id="btn_cancelrecord" name="" type="button" class="btn2" style="width:60px" value="取消" data-dismiss="modal">
		</div>
	</div>
</div>

<script type="text/javascript">
    $(function(){
        document.getElementById("recordCreateContent").placeholder = "请输入符合接口描述的规范JSON语法。例：";
    });

    function insertExample(type) {
        var recordContent = null;
        if (type == 1)
            recordContent = '{ \n   "records" : [     \n     {  \n      "col4" : "value4", \n      "col5" : "value5", \n      "primary_key":{ \n           "col1" : "value1", \n           "col2" : "value2", \n           "col3" : "value3" \n     } \n     ] \n }';

        $("#recordCreateContent").attr("value", recordContent);
    }

    function recordCreate(){
        $("#recordCreateStatus").html("");
        $.ajax({
            type: "POST",
            url: "/otscfgsvr/api/record/" + modal_table_name,
            data:$("#recordCreateContent").val(),
            dataType:"json",
            contentType: "application/json",
            timeout:30000,
            success:function(results, msg){
                $("#recordCreateStatus").append("添加记录" + errorInfo(results["errcode"]));
                getRecordList();
            },
            error: function(msg){
                var errmsg = "添加记录失败！错误: " + getStructMsg(msg);
                $("#recordCreateStatus").append(errmsg);
            }
        });
    }

</script>
