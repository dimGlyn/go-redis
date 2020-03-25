package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v7"
)

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
