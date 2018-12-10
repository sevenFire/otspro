	function isValidName(name) {
		if (name == null) {
			return false;
		}
		
		//名称只能包含拉丁字母或数字或下划线“_”，只能以字母开头，且不能以下划线“_”开头和结尾，且不能包含连续下划线“_”。
	 	var rex_check_str_1 = /^([_A-Z0-9a-z])+$/;	
		var rex_check_str_2 = /^((?!_{2,})[A-Za-z](([A-Z0-9a-z])*(_)?([A-Z0-9a-z])+)*)$/;
		if(!rex_check_str_1.test(name)) {
			return false;
		}
		if(!rex_check_str_2.test(name)) {
			return false;
		}
		return true;
	}

