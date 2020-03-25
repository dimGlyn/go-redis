package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

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
