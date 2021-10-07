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
	var address string
	ts := int64(0)
	for _, s := range data {
		address = s["address"].(string)
		ts = int64(s["timestamp"].(float64))
	}
	cron := cron.New()
	writeTime := "*/10 * * * *"
	log.Printf("DB write time : %v", writeTime)
	cron.AddFunc(writeTime, func() {
		WriteDB(ts, address)
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

func GetJSONData() []map[string]interface{} {
	resp, err := http.Get("https://eth-pps.baikalmine.com/api/payments")
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

	stats := result["payments"].([]interface{})
	var results []map[string]interface{}
	for _, s := range stats {
		message := make(map[string]interface{})
		data := s.(map[string]interface{})
		timestamp := data["timestamp"]
		address := data["address"]
		message["address"] = address
		message["timestamp"] = timestamp
		results = append(results, message)
	}

	return results
}

func WriteDB(timestamp int64, address string) error {

	connStr := "user=postgres password=1 dbname=test sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err1 := db.Exec("insert into testtable (timestamp, Prices) values ($1, $2)", timestamp, address)
	if err1 != nil {
		panic(err1)
	}

	start := time.Now()
	log.Printf("Write data to DB --->>> %s", time.Since(start))

	return err1
}
