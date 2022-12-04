package main

import (
	"bufio"
	"fmt"
	"github.com/yungsem/gox/filex"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

const (
	dbTypeMysql  = "mysql"
	dbTypeMssql  = "mssql"
	dbTypeOracle = "oracle"
)

// execDiffChangeLogForDDL 执行 liquibase diffChangeLog 命令
// 比对两个指定数据库中的表结构，生成相应的 changelog-ddl.xml 文件
func execDiffChangeLogForDDL() {
	// 执行 liquibase diffChangeLog 命令
	cmd := exec.Command("liquibase\\liquibase",
		"--changeLogFile=changelog/changelog-ddl.xml",
		"--defaultsFile=config/liquibase-ddl.properties",
		"diffChangeLog")
	out, err := cmd.CombinedOutput()
	Log.Debug(string(out))
	if err != nil {
		Log.ErrorE(err)
	}
}

// execUpdateSql 执行 liquibase updateSql 命令
// 根据 dbType 指定的数据类型，将 changelog.xml 转换成对应的 sql 脚本
func execUpdateSql(dbType string) {
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
	cmd := exec.Command("liquibase\\liquibase",
		"--changeLogFile=changelog/changelog-ddl.xml",
		confFilePath,
		"updateSql")
	out, err := cmd.CombinedOutput()
	Log.Debug(string(out))
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// 创建临时文件 xxxTemp
	file, err := filex.OpenFile(dbType + "DDLTemp")
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
	Log.DebugF("successfully write %d bytes to %s file", n, dbType+"Temp")
}

// resolveDDLFromTempFile 从 xxxTemp 文件中提取 DDL
func resolveDDLFromTempFile(dbType string) {
	// 先删除 out 目录
	outDir := "out/ddl/"
	err := os.RemoveAll(outDir)
	if err != nil {
		Log.ErrorE(err)
	}
	// 创建 out 目录，用于存放最终的 sql 文件
	err = os.MkdirAll(outDir, 0666)
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// 创建 xxx.sql 文件
	sqlFile, err := filex.OpenFile(outDir + dbType + ".sql")
	if err != nil {
		Log.ErrorE(err)
		return
	}
	defer sqlFile.Close()

	// 打开 xxxTemp 文件，开始提取 DDL
	tempFile, err := os.Open(dbType + "DDLTemp")
	if err != nil {
		Log.ErrorE(err)
		return
	}

	// 提取完成之后删除 xxxTemp 文件
	defer func() {
		rErr := os.RemoveAll(dbType + "DDLTemp")
		if rErr != nil {
			Log.ErrorE(rErr)
		}
	}()

	defer tempFile.Close()

	// 逐行读取文件内容，提取需要的
	flag := false
	scanner := bufio.NewScanner(tempFile)
	for scanner.Scan() {
		if flag {
			if strings.Contains(scanner.Text(), "INSERT INTO") {
				flag = false
			} else {
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
				Log.DebugF("write %d bytes to %s file", n, dbType+".sql")
			}
			continue
		}
		if strings.Contains(scanner.Text(), "Changeset") {
			flag = true
		}
	}
}

// typeMappingMysql 转换 mysql 的数据类型
// 自动生成的数据类型有时候不满足现状
func typeMappingMysql(sql string) string {
	// 去除表名
	sql = removeTableName(sql, dbTypeMysql)
	return sql
}

// typeMappingOracle 转换 oracle 的数据类型
// 自动生成的数据类型有时候不满足现状
func typeMappingOracle(sql string) string {
	// VARCHAR2(xx) -> VARCHAR2(xx char)
	varchar2Reg := regexp.MustCompile(`VARCHAR2\([0-9]+\)`)
	varchar2Arr := varchar2Reg.FindStringSubmatch(sql)
	for _, varchar2 := range varchar2Arr {
		newStr := varchar2[0:len(varchar2)-1] + " char)"
		sql = strings.ReplaceAll(sql, varchar2, newStr)
	}
	// DECIMAL -> NUMBER
	sql = strings.ReplaceAll(sql, "DECIMAL", "NUMBER")

	// 去除表名
	sql = removeTableName(sql, dbTypeOracle)

	return sql
}

// typeMappingSqlServer 转换 sqlServer 的数据类型
// 自动生成的数据类型有时候不满足现状
func typeMappingSqlServer(sql string) string {
	// varchar -> nvarchar
	sql = strings.ReplaceAll(sql, "varchar", "nvarchar")
	sql = strings.ReplaceAll(sql, "nnvarchar", "nvarchar")
	// varchar (max) -> ntext
	sql = strings.ReplaceAll(sql, "varchar (max)", "ntext")
	sql = strings.ReplaceAll(sql, "varchar(MAX)", "ntext")
	sql = strings.ReplaceAll(sql, "nntext", "ntext")
	// datetime -> datetime2
	sql = strings.ReplaceAll(sql, "datetime", "datetime2")

	return sql
}

// removeTableName 去除表名
func removeTableName(sql string, dbType string) string {
	f, err := os.Open(fmt.Sprintf("config/liquibase-update-%s.properties", dbType))
	if err != nil {
		Log.ErrorE(err)
	}
	defer f.Close()

	// 逐行读取文件内容，提取需要的
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "db") {
			arr := strings.Split(line, ": ")
			if len(arr) == 2 {
				sql = strings.ReplaceAll(sql, arr[1]+".", "")
			}
		}
	}
	return sql
}
