package main

import (
	"github.com/yungsem/gox/logx"
	"os"
	"sync"
)

var Log = logx.NewStdoutLog(logx.InfoStr)

func main() {
	// 创建 changelog 目录，存放 changelog-ddl.cml 文件
	err := os.Mkdir("changelog", 0666)
	if err != nil {
		Log.ErrorE(err)
	}

	// 方法结束时删除 changelog 目录
	defer func() {
		rErr := os.RemoveAll("changelog")
		if rErr != nil {
			Log.ErrorE(err)
		}
	}()

	// 执行 liquibase diffChangeLog 命令
	Log.Info("=====>>>start creating changelog-ddl.xml")
	execDiffChangeLogForDDL()

	// 执行 liquibase diffChangeLog 命令
	Log.Info("=====>>>start creating changelog-dml.xml")
	execDiffChangeLogForDML()

	dbTypes := []string{
		dbTypeMysql,
		//dbTypeMssql,
		//dbTypeOracle,
	}

	var wg sync.WaitGroup
	for _, dbType := range dbTypes {
		// ddl
		wg.Add(1)
		go func(dbType string) {
			defer wg.Done()
			Log.InfoF("=====>>>generate sql(ddl) for %s", dbType)
			execUpdateSql(dbType)

			Log.InfoF("=====>>>resolve ddl for %s", dbType)
			resolveDDLFromTempFile(dbType)
		}(dbType)

		// dml
		wg.Add(1)
		go func(dbType string) {
			defer wg.Done()
			Log.InfoF("=====>>>generate sql(dml) for %s", dbTypeMysql)
			execUpdateSqlForDML(dbTypeMysql)

			Log.InfoF("=====>>>resolve dml for %s", dbTypeMysql)
			resolveDMLFromTempFile(dbTypeMysql)
		}(dbType)
	}

	wg.Wait()
}
