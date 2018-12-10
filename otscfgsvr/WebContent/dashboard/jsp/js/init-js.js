
var navPageItem = ['table', 'metrics', 'helper'];
function showActivePage(pageItem)
{
	$("#"+pageItem+" a").css("border-bottom", "3px solid rgb(41,171, 226)");
	$("#"+pageItem + " p").css("color", "#8c8c8c");
	for (var i = 0; i < navPageItem.length; i++)
	{
		if (navPageItem[i] != pageItem)
		{
			$("#"+pageItem + " a").css("border-bottom", null);
			$("#"+pageItem + " p").css("color", "#ffffff");
		}
	}
}

function getUrlParam(name)
{
	var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)"); 
	var r = window.location.search.substr(1).match(reg);  
	if (r!=null) 
		return unescape(r[2]); 
	return null; 
}

function setUrlParam(param,value){
    var query = location.search.substring(1);
    var p = new RegExp("(^|&"+param+")=[^&]*");
    if(p.test(query)){
        query = query.replace(p,"$1="+value);
        location.search = '?'+query;
    }else{
        if(query == ''){
            location.search = '?'+param+'='+value;
        }else{
            location.search = '?'+query+'&'+param+'='+value;
        }
    }    
}

function setContentMinHeight()
{
	var viewport_height=$(window).height();
	var content_height = $(".content").height();
	if (content_height < viewport_height-180)
	{
		$(".content").css("min-height",viewport_height-180);
	}
}

$(function(){ 
    setContentMinHeight();
    $(window).resize(function(){
    	setContentMinHeight();
    	});
}); 

var keys = Object.keys || function(obj) {
    obj = Object(obj);
    var arr = [];
    for (var a in obj) arr.push(a);
    return arr;
};
var invert = function(obj) {
    obj = Object(obj);
    var result = {};
    for (var a in obj) result[obj[a]] = a;
    return result;
};
var entityMap = {
    escape: {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;',
      ' ': '&nbsp;'
    }
};
entityMap.unescape = invert(entityMap.escape);
var entityReg = {
    escape: RegExp('[' + keys(entityMap.escape).join('') + ']', 'g'),
    unescape: RegExp('(' + keys(entityMap.unescape).join('|') + ')', 'g')
};
 

function htmlEscape(html) {
    if (typeof html !== 'string') return html;
    return html.replace(entityReg.escape, function(match) {
        return entityMap.escape[match];
    });
}

function htmlUnescape(str) {
    if (typeof str !== 'string') return str;
    return str.replace(entityReg.unescape, function(match) {
        return entityMap.unescape[match];
    });  
}

//字段不存在时赋值为""
function isDefine(a){
	if (typeof(a) == "undefined") {		
		a="";
	}
	return a;
}

//组织格式化提示信息
function getStructMsg(msg) {
	var retMsg = msg["status"];
	if (msg["statusText"]) {
		retMsg += " - " + msg["statusText"];
	}
	//if not error code ourself, only add httpstatusText; otherwise add our error message.
   	if (msg["status"] == 400) {
   		var self = isDefine(msg["responseText"]);		
   		var pos = String(self).indexOf("(ErrorCode:");
   		if (pos >= 0) {
   			//retMsg += ". " + self;
   			if (String(self).indexOf("\n") > 0)
   				retMsg += ". " + self.substring(pos, String(self).indexOf("\n"));
   			else
   				retMsg += ". " + self;
   		} 
   	}   
   	
   	//401 auth check
 	if(msg["status"] == 401)
 		retMsg += "<br/>(请刷新页面或者重新登录！)";
 	
 	return retMsg;
}

//判断数组内是否有重复元素
	function isRepeat(arr) {
		var hash = {};
		for (var i in arr) {
			if (hash[arr[i]])
				return true;
			hash[arr[i]] = true;
		}
		return false;
	}
	
//判断数组内是否有重复元素
	function isGtMax(num) {
		if (num > Math.pow(2,53)) 
			return true;
		else
			return false;
	}
	
//table format
	var sprintf = function(str) {
        var args = arguments,
            flag = true,
            i = 1;
        str = str.replace(/%s/g, function () {
            var arg = args[i++];

            if (typeof arg === 'undefined') {
                flag = false;
                return '';
            }
            return arg;
        });
        return flag ? str : '';
    };
    
//页面显示名称少于20个字符
	function getStructName(name) {
		if (name != null) {

			if (name.length <= 20){
	       		return name;
	   		}
	   		else {
	   			return name.substring(0,20) + "...";
	   		}
		}
		else {
			return name;
		}
	}
	
//ajax: all buttons disabled
	$(document).ajaxStart(function() {
		var btns = document.getElementsByClassName("btn2");
		var btn_index_view = document.getElementById("btn_queryindexrec");
		//var btn3s = document.getElementsByClassName("btn3");
		for(var i=0;i<btns.length;i++){
			btns[i].style.cursor="wait";
		   	btns[i].disabled = true; 
		}
		if (btn_index_view != null) {
			btn_index_view.style.cursor="wait";
			btn_index_view.disabled = true;
		}
		
		document.body.style.cursor="wait";
	});
	$(document).ajaxComplete(function() {
		var btns = document.getElementsByClassName("btn2");
		var btn_index_view = document.getElementById("btn_queryindexrec");
		//var btn3s = document.getElementsByClassName("btn3");
		for(var i=0;i<btns.length;i++){
			btns[i].style.cursor="pointer";
			btns[i].disabled = false; 
		}
		if (btn_index_view != null) {
			btn_index_view.style.cursor="pointer";
			btn_index_view.disabled = false;
		}
		document.body.style.cursor="default";
	});
