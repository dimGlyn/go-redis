package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/go-redis/redis/v7"
)

var client *redis.Client

type shortURL struct {
	Hits         string `json:"hits"`
	ShortURLCode string `json:"shortURLCode"`
	TargetURL    string `json:"targetURL"`
	Created      string `json:"created"`
	LastHit      string `json:"lastHit"`
}

type shortUrlsData struct {
	Type         string   `json:"type"`
	ShortURLData shortURL `json:"value"`
}

type JSONData []map[string]shortUrlsData

type campaign struct {
	campaignID, marketID string
}

var campaigns []campaign

func init() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})

	err := parseCampaignCSV("campaignMarketAssign.csv")

	parseJSONshortURLs("test.json")

	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
}

func parseCampaignCSV(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	campaigns = []campaign{}

	for _, record := range records[1:] {
		campaigns = append(campaigns, campaign{
			campaignID: record[0],
			marketID:   record[1],
		})
	}

	return nil
}

func parseJSONshortURLs(filepath string) error {
	file, _ := ioutil.ReadFile(filepath)

	fmt.Println(file)

	var shortUrls JSONData

	_ = json.Unmarshal([]byte(file), &shortUrls)

	fmt.Println(shortUrls[0]["joi:shortURLCodes:jEOeTEd9"].ShortURLData)

	return nil
}

func getMarket(campaignID string) (string, error) {
	for _, camp := range campaigns {
		if camp.campaignID == campaignID {
			return camp.marketID, nil
		}
	}
	return "nil", errors.New("Invalid campaignID")
}
