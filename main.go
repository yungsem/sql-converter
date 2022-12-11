package main

import (
	"github.com/yungsem/gox/filex"
	"github.com/yungsem/gox/logx"
	"sync"
)

// var Log = logx.NewFileLog(logx.DebugStr, "logs")
var Log = logx.NewStdoutLog(logx.DebugStr)

const (
	WorkDir      = "workdir"
	WorkDirDDL   = "workdir/ddl"
	WorkDirDML   = "workdir/dml"
	ChangeLogDir = WorkDir + "/changelog/"
	TempDirDDL   = WorkDirDDL + "/temp/"
	TempDirDML   = WorkDirDML + "/temp/"
	OutDirDDL    = WorkDirDDL + "/out/"
	OutDirDML    = WorkDirDML + "/out/"
)

func main() {
	// 清空或创建 workdir/changelog 目录，存放 changelog-ddl.cml 文件
	err := filex.ClearOrMakeDir(ChangeLogDir)
	if err != nil {
		Log.ErrorE(err)
	}

	// 执行 liquibase diffChangeLog 命令
	Log.Info("start creating changelog-ddl.xml")
	execDiffChangeLogForDDL()

	// 执行 liquibase diffChangeLog 命令
	Log.Info("start creating changelog-dml.xml")
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
			Log.InfoF("generate sql(ddl) for %s", dbType)
			execUpdateSqlForDDL(dbType)

			Log.InfoF("resolve ddl for %s", dbType)
			resolveDDLFromTempFile(dbType)
		}(dbType)

		// dml
		wg.Add(1)
		go func(dbType string) {
			defer wg.Done()
			Log.InfoF("generate sql(dml) for %s", dbType)
			execUpdateSqlForDML(dbType)

			Log.InfoF("resolve dml for %s", dbType)
			resolveDMLFromTempFile(dbType)
		}(dbType)
	}

	wg.Wait()
}
