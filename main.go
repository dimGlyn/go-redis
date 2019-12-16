package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/go-redis/redis/v7"
)

var clientWallaces *redis.Client
var clientEndeavors *redis.Client
var clientAngler *redis.Client

type shortURL struct {
	Hits         string `json:"hits"`
	ShortURLCode string `json:"shortURLCode"`
	TargetURL    string `json:"targetURL"`
	Created      string `json:"created"`
	LastHit      string `json:"lastHit"`
}

type shortUrlsData struct {
	Type string   `json:"type"`
	data shortURL `json:"value"`
}

type jsonData map[string]shortUrlsData

type campaign struct {
	campaignID, marketID string
}

var campaigns []campaign
var data []jsonData

func init() {
	clientWallaces = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})

	parseCampaignCSV("campaignMarketAssign.csv")
	parseJSONshortURLs("dump.json")
}

func main() {
	pong, err := clientWallaces.Ping().Result()
	fmt.Println(pong, err)

	for i, chunk := range data {
		fmt.Printf("proccess chunk %d\n", i)
		proccessChunk(chunk)
	}
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

	_ = json.Unmarshal([]byte(file), &data)

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

func proccessChunk(chunk jsonData) {
	fmt.Println(chunk["joi:shortURLCodes:dq79oUVt"])
	shorturl := chunk["joi:shortURLCodes:dq79oUVt"]
	targeturl := shorturl.data.TargetURL

	if strings.Contains(targeturl, "/ca/0") {
		handleOptinUrl(shorturl)
	} else {
		handleCampaignUrl(shorturl)
	}

	// for k, value := range chunk {
	// 	fmt.Println("joi:shortURLCodes:dq79oUVt")
	// }
}

func handleCampaignUrl(shorturl shortUrlsData) {

}

func handleOptinUrl(shorturl shortUrlsData) {

}
