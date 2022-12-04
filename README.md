## 思路
对比 MySQL 两个库中表结构的差异，生成 changelog-ddl.xml 文件，命令如下：
```shell
liquibase --changeLogFile=changelog/changelog-ddl.xml --defaultsFile=config/liquibase-ddl.properties diffChangeLog
```
