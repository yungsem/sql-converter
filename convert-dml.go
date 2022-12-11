package main

import (
	"bufio"
	"github.com/yungsem/gox/filex"
	"os"
	"os/exec"
	"strings"
)

// execDiffChangeLogForDML 执行 liquibase diffChangeLog 命令
// 比对两个指定数据库中的表结构，生成相应的 changelog-ddl.xml 文件
func execDiffChangeLogForDML() {
	// 执行 liquibase diffChangeLog 命令
	cmd := exec.Command("liquibase",
		"--diff-types=data",
		"--changeLogFile="+ChangeLogDir+"changelog-dml.xml",
		"--defaultsFile=config/liquibase-dml.properties",
		"--dataOutputDirectory="+ChangeLogDir,
		"generate-changelog")
	out, err := cmd.CombinedOutput()
	Log.Debug(string(out))
	if err != nil {
		Log.ErrorE(err)
	}
}

// execUpdateSqlForDML 执行 liquibase updateSql 命令
// 根据 dbType 指定的数据类型，将 changelog.xml 转换成对应的 sql 脚本
func execUpdateSqlForDML(dbType string) {
	// 构建 confFilePath
	var confFilePath string
	switch dbType {
	case dbTypeMysql:
		confFilePath = "--defaultsFile=config/liquibase-update-mysql.properties"
	case dbTypeMssql:
		confFilePath = "--defaultsFile=config/liquibase-update-sqlserver.properties"
	case dbTypeOracle:
		confFilePath = "--defaultsFile=config/liquibase-update-oracle.properties"
	default:
		confFilePath = "--defaultsFile=config/liquibase-update-mysql.properties"
	}

	// 执行 liquibase updateSql 命令
	cmd := exec.Command("liquibase",
		"--changeLogFile="+ChangeLogDir+"changelog-dml.xml",
		confFilePath,
		"updateSql")
	out, err := cmd.CombinedOutput()
	Log.Debug(string(out))
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// 清空或创建 workdir/temp 目录，存放 .ddl 文件
	err = filex.ClearOrMakeDir(TempDirDML + dbType)
	if err != nil {
		Log.ErrorE(err)
	}

	file, err := filex.MakeFile(TempDirDML + dbType + "/dml.temp")
	if err != nil {
		Log.ErrorE(err)
		return
	}
	defer file.Close()

	// 将 liquibase updateSql 命令的输出写入 xxxTemp 文件中
	n, err := file.Write(out)
	if err != nil {
		Log.ErrorE(err)
		return
	}
	Log.DebugF("successfully write %d bytes to %s file", n, TempDirDML+dbType+"/dml.temp")
}

// resolveDMLFromTempFile 从 xxxTemp 文件中提取 DDL
func resolveDMLFromTempFile(dbType string) {
	// 清空或创建 workdir/out 目录，存放 .ddl 文件
	err := filex.ClearOrMakeDir(OutDirDML + dbType)
	if err != nil {
		Log.ErrorE(err)
	}

	// 创建 xxx.sql 文件
	sqlFile, err := filex.MakeFile(OutDirDML + dbType + "/" + dbType + ".sql")
	if err != nil {
		Log.ErrorE(err)
		return
	}
	defer sqlFile.Close()

	// 打开 xxxTemp 文件，开始提取 DDL
	tempFile, err := os.Open(TempDirDML + dbType + "/dml.temp")
	if err != nil {
		Log.ErrorE(err)
		return
	}
	defer tempFile.Close()

	// 逐行读取文件内容，提取需要的
	scanner := bufio.NewScanner(tempFile)
	for scanner.Scan() {
		if !strings.Contains(scanner.Text(), "INSERT INTO") {
			continue
		}
		if strings.Contains(scanner.Text(), "DATABASECHANGELOGLOCK") {
			continue
		}
		if strings.Contains(scanner.Text(), "DATABASECHANGELOG") {
			continue
		}
		// 类型映射
		var sql string
		switch dbType {
		case dbTypeMssql:
			sql = typeMappingSqlServer(scanner.Text())
		case dbTypeOracle:
			sql = typeMappingOracle(scanner.Text())
		case dbTypeMysql:
			sql = typeMappingMysql(scanner.Text())
		}
		// 开始提取内容
		n, wErr := sqlFile.WriteString(sql + "\n")
		if wErr != nil {
			Log.ErrorE(wErr)
		}
		Log.DebugF("write %d bytes to %s file", n, OutDirDML+dbType+"/"+dbType+".sql")
	}
}
