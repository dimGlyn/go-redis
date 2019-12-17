package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/go-redis/redis/v7"
)

var client1 *redis.Client
var client2 *redis.Client
var client3 *redis.Client

type shortURL struct {
	Hits         string `json:"hits"`
	ShortURLCode string `json:"shortURLCode"`
	TargetURL    string `json:"targetURL"`
	Created      string `json:"created"`
	LastHit      string `json:"lastHit"`
}

type shortUrlsData struct {
	Type string   `json:"type"`
	Data shortURL `json:"value"`
}

type jsonData map[string]shortUrlsData

type campaign struct {
	campaignID, marketID string
}

var campaigns []campaign
var data []jsonData

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
	parseJSONshortURLs("dump.json")
}

func main() {
	pong, err := client1.Ping().Result()
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
	fmt.Println(chunk["joi:shortURLCodes:DKJPeiXo"])
	shorturl := chunk["joi:shortURLCodes:DKJPeiXo"]
	targeturl := shorturl.Data.TargetURL

	if strings.Contains(targeturl, "/ca/0/") {
		handleOptinURL(shorturl)
	} else if strings.Contains(targeturl, "/ca/") {
		handleCampaignURL(shorturl)
	} else {
		fmt.Println(targeturl)
	}

	// for k, value := range chunk {
	// 	fmt.Println("joi:shortURLCodes:dq79oUVt")
	// }
}

func handleCampaignURL(shorturl shortUrlsData) {
	arr := strings.Split(shorturl.Data.TargetURL, "/")

	campaignID := arr[4]
	marketID, err := getMarket(campaignID)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := mapTenant(marketID)
	if err != nil {
		log.Fatalln(err)
	}

	insertToRedis(shorturl, client)
}

func handleOptinURL(shorturl shortUrlsData) {
	arr := strings.Split(shorturl.Data.TargetURL, "/")

	marketID := arr[6]

	client, err := mapTenant(marketID)
	if err != nil {
		log.Fatalln(err)
	}

	arr[6] = "3"

	shorturl.Data.TargetURL = strings.Join(arr, "/")

	insertToRedis(shorturl, client)
}

func mapTenant(marketID string) (*redis.Client, error) {

	switch marketID {
	case "1":
		return client1, nil
	case "6":
		return client3, nil
	case "8":
		return client2, nil
	}
	return nil, errors.New("Tenant not found")
}

func insertToRedis(shorturl shortUrlsData, client *redis.Client) error {
	fmt.Println(shorturl)
	fmt.Println(client)
	return nil
}
