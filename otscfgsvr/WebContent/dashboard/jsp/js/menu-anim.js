/*---选择元素函数---*/
function $$(id) {
    //alert(document.getElementById(id));
    return document.getElementById(id);
}

/*--个人主页菜单选项动画---*/
var menuStatus = new Array();
var activeW = 0;
var leftMargin = new Array();
var num = 0;
var menuInitFlag = 0;
var myMar;
var menuObj = new Array();
var pageObj = new Array();

function menuInit()
{
	if (0 == menuInitFlag)
	{
		getObj();
		$("#" + menuObj[0].id).css("border-bottom", "3px solid rgb(41,171, 226)");
	    var menuWidth = 90 * pageObj.length;
	    $$("menuType").style.width = menuWidth + "px";
	    activeW = getMenuLeftMargin(menuWidth);
	    for (var i = 0; i < pageObj.length; i++) {
	        leftMargin[i] = activeW + 90 * i;
	    }
	    menuInitFlag = 1;
	}
}

function getObj() {
    menuObj = $$("menuType").children;
    //alert(menuObj.length);
    pageObj = $$("pageGroup").children;
    //alert(pageObj.length);
}

function menuClick(num, sum) {
	menuInit();

    for (var i = 0; i < sum; i++) {
        menuStatus[i] = 0;
    }

    menuStatus[num] = 1;
    setPage(num);

    
    for (var i = 0; i < sum; i++) {
        if (menuStatus[i] == 1) {
            $("#" + menuObj[i].id).css("border-bottom", "3px solid rgb(41,171, 226)");
        }
        else {
            $("#" + menuObj[i].id).css("border-bottom", 'none');
        }
    }
}

function getMenuLeftMargin(width) {
    var topWidth = $$("menuTop").offsetWidth;
    var leftMargin = (topWidth - width) / 2 - 4;
    //alert(leftMargin);
    return leftMargin;
}

function moveIndexBar(i) {
    //alert("moving");
    num = i;
    myMar = setInterval(moveIng, 5);
}

function moveIng() {
    if (activeW < leftMargin[num] - 9) {
        activeW += parseInt((leftMargin[num] - activeW) * 0.1);
        //alert("activeW:"+activeW);
        $$("indexBar").style.left = activeW + 9 + "px";
        //alert("indexBar:"+ $("indexBar").style.left);
        //alert("leftMargin:"+leftMargin[num]);
    }
    else if (activeW > leftMargin[num] + 9) {
        activeW += parseInt((leftMargin[num] - activeW) * 0.1);
        //alert("activeW:"+activeW);
        $$("indexBar").style.left = activeW - 9 + "px";
    }
    else {
        clearInterval(myMar);
    }
}

function setPage(count) {
    for (var i = 0; i < pageObj.length; i++) {
        if (i == count) {
            pageObj[i].style.display = "block";
        }
        else {
            pageObj[i].style.display = "none";
        }
    }
}

window.onload = function () {
	menuInit();
};