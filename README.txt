文件说明：
${env.VOBBASE_Home}\xInsight\Source\ots\otspro\build_otspro.xml           ant文件
${env.VOBBASE_Home}\xInsight\Source\ots\otspro\otspro_version.properties  版本定义文件
${env.VOBBASE_Home}\xInsight\Source\ots\otspro\otspro_path.properties     路径定义文件
${env.VOBBASE_Home}\xInsight\Script\build\build_otspro.bat   编译脚本
${env.VOBBASE_Home}\xInsight\Script\build\copy\otspro.bat  build_otspro.xml调用的脚本，用以复制依赖。


1.设置环境变量(在otspro_path.properties中用到)
DeliveryBase_Home=???
VOBBASE_Home=C:\Users\liyuhui\lyh\workspaces\ws_xinsight
JAVA7_HOME=C:\Users\liyuhui\lyh\software\java\jdk1.8.0_151

2.编译项目
2.1或2.2任选一种。

2.1运行build_otspro.bat

2.2 手动编译
cd %VOBBASE_Home%\xInsight\source\web\compile\packages
mvn clean install

cd %VOBBASE_Home%\xInsight\Source\common
mvn clean install -DskipTests

cd %VOBBASE_Home%\xInsight\Source\ots\otspro
mvn clean install -Pcompile -DskipTests

cd %VOBBASE_Home%\xInsight\Source\ots\otspro
ant -buildfile build_otspro.xml

cd %VOBBASE_Home%\xInsight\script\build
