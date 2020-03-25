package main

import (
	"strings"

	"github.com/go-redis/redis/v7"
)

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
