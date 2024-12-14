package main

import (
	"fmt"
	"github.com/adzimzf/sqlite-go/db"
	"github.com/adzimzf/sqlite-go/executor"
	"log"
	"os"
	"strings"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	databaseHeader, err := db.ReadDatabaseHeader(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	rootPage, err := db.NewTableLeafPage(databaseFile, int(databaseHeader.PageSize), 1)
	if err != nil {
		log.Fatal(err)
	}

	records, err := rootPage.GetRecords()
	if err != nil {
		log.Fatal(err)
	}

	// in sqlite the 3rd argument will doesn't start with a dot (.) it's a sql query.
	if !strings.HasPrefix(command, ".") {

		sqlInfo, err := db.ExtractQueryInfo(command)
		if err != nil {
			log.Println(err)
		}
		rows, err := executor.ExecuteSelectQuery(databaseFile, sqlInfo)
		if err != nil {
			return
		}

		fmt.Println(rows.RowsString(sqlInfo.SelectFields))

		return
	}
	switch command {
	case ".dbinfo":

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Println("Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", databaseHeader.PageSize)

		fmt.Printf("number of tables: %v\n", len(records))
	case ".tables":
		for _, record := range records {
			data, f, err := record.FieldData(0)
			if err != nil {
				log.Fatal(err)
			}
			if f != db.String {
				log.Fatal("the first column type isn't blob")
			}

			tableType := data.(string)
			if tableType != "table" {
				continue
			}

			fieldData, _, err := record.FieldData(2)
			if err != nil {
				log.Fatal(err)
			}

			// skip the internal tables
			if strings.HasPrefix(fieldData.(string), "sqlite_") {
				continue
			}

			fmt.Printf("%s ", fieldData.(string))
		}
		fmt.Printf("\n")
	default:

		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
