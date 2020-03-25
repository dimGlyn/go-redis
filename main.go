package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-redis/redis/v7"
)

var client1 *redis.Client
var client2 *redis.Client
var client3 *redis.Client

var campaigns []campaign
var data []jsonData
var usedCodesData usedCodesJSONData
var maxCampaign int

var logFile *os.File

func init() {
	client1 = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       2,
	})

	client2 = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       3,
	})

	client3 = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       4,
	})

	parseCampaignCSV("campaignMarketAssign.csv")
	fmt.Println("Parsed campaign file")

	parseshortURLsJSON("dump.json")
	fmt.Println("Parsed shortURL file")

	parseUsedCodesJSON("usedCodes.json")
	fmt.Println("Parsed shortURL file")
	var err error
	logFile, err = os.Create("log.txt")
	if err != nil {
		logFile.WriteString("ERROR " + err.Error() + "\n")
		return
	}
}

func main() {
	for i, chunk := range data {
		fmt.Printf("proccess chunk %d\n", i)
		proccessChunk(chunk)
	}
	for _, v := range usedCodesData {
		fmt.Println("Insert usedCodes")
		insertUsedCodesToRedis(v)
	}
	logFile.Close()
}

func proccessChunk(chunk jsonData) {
	for key := range chunk {
		shorturl := chunk[key]
		targeturl := shorturl.Data.TargetURL
		if strings.Contains(targeturl, "/ca/0/") {
			go handleOptinURL(shorturl)
		} else if strings.Contains(targeturl, "/ca/") {
			go handleCampaignURL(shorturl)
		} else {
			logFile.WriteString("Offer url" + targeturl + "\n")
		}
	}
}
