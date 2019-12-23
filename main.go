package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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
type usedCodes struct {
	Type string        `json:"type"`
	Data []interface{} `json:"value"`
}
type shortUrlsData struct {
	Type string   `json:"type"`
	Data shortURL `json:"value"`
}

type jsonData map[string]shortUrlsData
type usedCodesJSONData map[string]usedCodes

type campaign struct {
	campaignID, marketID string
}

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

func parseCampaignCSV(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	campaigns = []campaign{}

	for _, record := range records {
		campaigns = append(campaigns, campaign{
			campaignID: record[0],
			marketID:   record[1],
		})
	}

	maxCampaign = getMaxCampaign(campaigns)

	fmt.Println(maxCampaign)

	return nil
}

func getMaxCampaign(campaigns []campaign) (max int) {
	max, _ = strconv.Atoi(campaigns[0].campaignID)
	for _, c := range campaigns[1:] {
		cid, _ := strconv.Atoi(c.campaignID)
		if cid > max {
			max = cid
		}
	}
	return
}

func parseshortURLsJSON(filepath string) error {
	file, _ := ioutil.ReadFile(filepath)

	_ = json.Unmarshal([]byte(file), &data)

	return nil
}

func parseUsedCodesJSON(filepath string) error {
	file, _ := ioutil.ReadFile(filepath)

	err := json.Unmarshal([]byte(file), &usedCodesData)

	if err != nil {
		logFile.WriteString("ERROR " + err.Error() + "\n")
	}
	return nil
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

func handleCampaignURL(shorturl shortUrlsData) {
	arr := strings.Split(shorturl.Data.TargetURL, "/")

	campaignID := arr[4]

	i, err := strconv.Atoi(campaignID)
	if i > maxCampaign {
		return
	}
	marketID, err := getMarket(campaignID)
	if err != nil {
		logFile.WriteString("ERROR " + err.Error() + "\n" + shorturl.Data.ShortURLCode + "\n")
	}

	client, err := mapTenant(marketID)
	if err != nil {
		logFile.WriteString("ERROR " + err.Error() + "\n" + shorturl.Data.ShortURLCode + "\n")
		return
	}

	insertShortCodeToRedis(shorturl, client)
}

func handleOptinURL(shorturl shortUrlsData) {
	arr := strings.Split(shorturl.Data.TargetURL, "/")

	marketID := arr[6]

	client, err := mapTenant(marketID)
	if err != nil {
		logFile.WriteString("ERROR " + err.Error() + "\n" + shorturl.Data.ShortURLCode + "\n")
		return
	}

	arr[6] = "3"

	shorturl.Data.TargetURL = strings.Join(arr, "/")

	insertShortCodeToRedis(shorturl, client)
}

func getMarket(campaignID string) (string, error) {
	for _, camp := range campaigns {
		if camp.campaignID == campaignID {
			return camp.marketID, nil
		}
	}
	logFile.WriteString("ERROR " + "campaignID " + campaignID + "\n")
	return "nil", errors.New("Invalid campaignID")
}

func mapTenant(marketID string) (*redis.Client, error) {

	switch marketID {
	case "1":
		return client1, nil
	case "6":
		return client2, nil
	case "8":
		return client3, nil
	}
	return nil, errors.New("Tenant not found")
}

func insertShortCodeToRedis(shorturl shortUrlsData, client *redis.Client) error {
	key := strings.Join([]string{"joi", "shortURLCodes", shorturl.Data.ShortURLCode}, ":")
	value := make(map[string]interface{})
	value["hits"] = shorturl.Data.Hits
	value["shortURLCode"] = shorturl.Data.ShortURLCode
	value["targetURL"] = shorturl.Data.TargetURL
	value["created"] = shorturl.Data.Created
	value["lastHit"] = shorturl.Data.LastHit

	client.HMSet(key, value)
	return nil
}

func insertUsedCodesToRedis(usedcodes usedCodes) error {
	key := "joi:shortURLCodes:usedCodes"

	strings := []string{}

	for _, v := range usedcodes.Data {
		strings = append(strings, v.(string))
	}

	for _, client := range []*redis.Client{client1, client2, client3} {
		client.SAdd(key, strings)
	}
	return nil
}
