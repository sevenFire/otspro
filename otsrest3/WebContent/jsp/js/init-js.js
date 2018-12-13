
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

//组织格式化提示信�?
function getStructMsg(msg){
	var retMsg = msg["status"];
	if (msg["statusText"]) {
		retMsg += " - " + msg["statusText"];
	}
	//if not error code ourself, only add httpstatusText; otherwise add our error message.
   	if (msg["status"] == 400) {
   		var self = isDefine(msg["responseText"]);		
   		var pos = String(self).indexOf("(ErrorCode:");
   		if (pos == 0) {
   			//retMsg += ". " + self;
   			if (String(self).indexOf("\n") > 0)
   				retMsg += ". " + self.substring(0, String(self).indexOf("\n"));
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
