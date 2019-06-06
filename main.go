package main

import (
	"context"
	"database/sql"
	"log"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"

	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type SaasDbDetail struct {
	DbHost               string   `json:"db_host"`
	CustomerSchemaCount  int      `json:"customer_schema_count"`
	CustomerSchemas      []string `json:"customer_schemas"`
	AvailableSchemaSlots int      `json: "available_schema_slots`
}

//Global variables to entire program

var (
	wg        sync.WaitGroup
	sess, err = session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")})
	sqlStatement   = `SELECT datname FROM pg_catalog.pg_database`
	maxSchemaCount = 5
)

func SaaSDbHandler(ctx context.Context) ([]SaasDbDetail, error) {
	fmt.Println("Lambda handler called")
	dbDetails, err := fetchDetails()
	if err != nil {
		return nil, err
	}
	return dbDetails, nil
}

func main() {
	lambda.Start(SaaSDbHandler)
	//fetchDetails()
}

func fetchDetails() ([]SaasDbDetail, error) {
	// Create a New rds service
	rdsSvc := rds.New(sess)
	// Call to get detailed information on each DBinstance
	result, err := rdsSvc.DescribeDBInstances(nil)
	if err != nil {
		fmt.Println("Error", err)
		return nil, err
	}

	var dbDetails []SaasDbDetail
	// Adding lengths of dbInstances to wait group
	dbInstancesCount := len(result.DBInstances)
	wg.Add(dbInstancesCount)
	// Create a c hannel for final result.
	var SaasDBDetailChannel = make(chan SaasDbDetail, dbInstancesCount)
	for _, db := range result.DBInstances {
		go fetchDBDetails(db, SaasDBDetailChannel)
	}

	go func() {
		wg.Wait()
		close(SaasDBDetailChannel)
	}()

	for dbDetail := range SaasDBDetailChannel {
		log.Println("DB DETAIL IN CHANNEL RECEIVED: ", dbDetail)
		dbDetails = append(dbDetails, dbDetail)
	}

	return dbDetails, nil
}

func fetchDBDetails(dbInstance *rds.DBInstance, SaasDBDetailChannel chan SaasDbDetail) {
	defer wg.Done()
	dbHost := *dbInstance.Endpoint.Address

	var schemas []string
	if strings.HasPrefix(dbHost, "db-") {
		log.Println(dbHost)
		rows, err := execSqlQuery(sqlStatement, dbHost)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var schemaName string
			err := rows.Scan(&schemaName)
			if err != nil {
				panic(err)
			}
			if strings.Contains(schemaName, "dg") {
				schemas = append(schemas, schemaName)
			}
		}
		custSchemaCount := len(schemas)
		availableSlots := maxSchemaCount - custSchemaCount
		// fmt.Println(custSchemaCount)
		// fmt.Println(schemas)
		// fmt.Println(availableSlots)
		dbDetail := SaasDbDetail{DbHost: dbHost, CustomerSchemas: schemas, CustomerSchemaCount: custSchemaCount, AvailableSchemaSlots: availableSlots}
		SaasDBDetailChannel <- dbDetail
	}
}

func execSqlQuery(q string, dbHost string) (*sql.Rows, error) {
	dbPort := 5432
	dbUser := "postgres"
	dbPass := "dataguise"
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPass, "postgres")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(sqlStatement)
	if err != nil {
		return nil, err
	}
	return rows, err
}
