package main

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/robfig/cron"
)

func init() {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func Cron() {
	data := GetJSONData()
	var address string
	ts := int64(0)
	for _, s := range data {
		address = s["address"].(string)
		ts = int64(s["timestamp"].(float64))
	}
	cron := cron.New()
	writeTime := "@every 24h"
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
	ParamsList := getEnvAsSlice("DATA", []string{}, ",")

	var results []map[string]interface{}
	for _, s := range stats {
		message := make(map[string]interface{})
		data := s.(map[string]interface{})
		timestamp := data["timestamp"]
		address := data["address"]
		message["address"] = address
		message["timestamp"] = timestamp

		var conf []map[string]interface{}
		for _, c := range stats {
			configData := c.(map[string]interface{})
			for _, d := range ParamsList {
				config := make(map[string]interface{})
				config["config"] = configData[d]
				conf = append(conf, config)
				message["config"] = conf
			}
		}

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

	_, err1 := db.Exec("insert into testtable (timestamp, address) values ($1, $2)", timestamp, address)
	if err1 != nil {
		panic(err1)
	}

	start := time.Now()
	log.Printf("Write data to DB --->>> %s", time.Since(start))

	return err1
}

func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}

	return defaultVal
}

func getEnvAsSlice(name string, defaultVal []string, sep string) []string {
	valStr := getEnv(name, "")

	if valStr == "" {
		return defaultVal
	}

	val := strings.Split(valStr, sep)

	return val
}
