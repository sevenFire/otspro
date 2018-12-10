<%@ page language="java" contentType="text/html; charset=UTF-8"  pageEncoding="UTF-8"%> 
<!---dialog: error tips--->
<div class="modal" id="MessageAlert" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
	<div class="modal-dialog">
	  <div class="modal-content">
			<div class="modal-header">
				<button type="button" class="close" data-dismiss="modal" aria-hidden="true"> &times;</button>
				<h4 class="modal-title" id="myModalLabel" style="text-align:left;font-weight:bold;">通知</h4>  </div>
		 <div class="modal-body" id="MessageAlertContent" style="height:80px; word-wrap:break-word;border-collapse:collapse"></div>
		 <div class="modal-footer">
		 	<button type="button" class="btn" id="btn_confirm" onclick="confirmFunc();">确定 </button>
			<button type="button" class="btn" data-dismiss="modal">关闭 </button>
		 </div>
	  </div>
	</div><!-- /.modal -->
</div>

<script type="text/javascript">
	var confirm_type = -1;		//0-table, 1-record, 2-index
	var confim_operate = -1;		//0-delete, 1-multiDelete 2-clear, 3-rebuild
	var confirm_param;
	
    function errorAlertMsg(msg)
    {
    	document.getElementById("btn_confirm").style.display = "none";
		$("#MessageAlertContent").html("");
		$("#MessageAlertContent").append(msg);
		$("#MessageAlert").modal("show");
    }
    
    function confirmAlertMsg(msg, type, operate, param)
    {
    	confirm_type = type;
    	confirm_operate = operate;
    	confirm_param = param;
    	document.getElementById("btn_confirm").style.display = "inline-block";
		$("#MessageAlertContent").html("");
		$("#MessageAlertContent").append(msg);
		$("#MessageAlert").modal("show");
    }
    
    function confirmFunc() {
    	$("#MessageAlert").modal("hide");
    	switch(confirm_type) {
    		case 0:
    			tableConfirm();
    			break;
    		case 1:
    			recordConfirm();
    			break;
    		case 2:
    			indexConfirm();
    			break;
    		default:
    			break;
    	}
    }
    
    function tableConfirm() {
    	switch(confirm_operate) {
    	case 0:
    		tableDelete(confirm_param);
    		break;
    	case 1:
    		tableMultiDelete(confirm_param);
    		break;
    	default:
    		break;
    	}
    }
    
    function recordConfirm() {
    	switch(confirm_operate) {
    	case 0:
    		recordDelete(confirm_param);
    		break;
    	case 1:
    		recordMultiDelete(confirm_param);
    		break;
    	case 2:
    		recordClear();
    		break;
    	default:
    		break;
    	}
    }
    
    function indexConfirm() {
    	switch(confirm_operate) {
    	case 0:
    		indexDelete(confirm_param);
    		break;
    	case 1:
    		indexMultiDelete(confirm_param);
    		break;
    	case 2:
    		indexClear(confirm_param["row"], confirm_param["index"]);
    		break;
    	case 3:
    		indexRebuild(confirm_param["row"], confirm_param["index"]);
    	default:
    		break;
    	}
    }
</script>