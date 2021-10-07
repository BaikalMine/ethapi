package main

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
	"github.com/robfig/cron"
)

func Cron() {
	data := GetJSONData()
	prices := data["Price_USD"]
	ts := data["Timestamp"]
	cron := cron.New()
	writeTime := "@every 24h"
	log.Printf("DB write time : %v", writeTime)
	cron.AddFunc(writeTime, func() {
		WriteDB(int64(ts.(float64)), prices.(float64))
	})
	cron.Start()
}

func main() {
	Cron()
	app := fiber.New()
	app.Get("/load", func(c *fiber.Ctx) error {

		msg := GetJSONData()
		bytesRepresentation, err := json.Marshal(msg)
		if err != nil {
			log.Fatalln(err)
		}
		return c.Send(bytesRepresentation)
	})
	app.Listen(":3000")
}

func GetJSONData() map[string]interface{} {
	resp, err := http.Get("https://api.etherscan.io/api?module=stats&action=ethprice&apikey=M2XTQNNNBNWZ42IA3T9PTSNXP7NHPCUEQB")
	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	var result map[string]interface{}
	jsonErr := json.Unmarshal(body, &result)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	stats := result["result"].(map[string]interface{})
	timestamp := stats["ethusd_timestamp"]
	price_usd := stats["ethusd"]

	message := make(map[string]interface{})
	message["TimeStamp"] = timestamp
	message["Price_USD"] = price_usd

	return message
}

func WriteDB(timestamp int64, prices float64) error {

	connStr := "user=postgres password=1 dbname=test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err1 := db.Exec("insert into testtable (timestamp, Prices) values ($1, $2)", timestamp, prices)
	if err1 != nil {
		panic(err1)
	}

	start := time.Now()
	log.Printf("Write data to DB --->>> %s", time.Since(start))

	return err1
}
