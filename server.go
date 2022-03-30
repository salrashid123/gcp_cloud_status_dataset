package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/http2"
)

type Event struct {
	ID           string    `json:"id"`
	Number       string    `json:"number"`
	Begin        time.Time `json:"begin"`
	Created      time.Time `json:"created"`
	End          time.Time `json:"end"`
	Modified     time.Time `json:"modified"`
	ExternalDesc string    `json:"external_desc"`
	Updates      []struct {
		Created           time.Time `json:"created"`
		Modified          time.Time `json:"modified"`
		When              time.Time `json:"when"`
		Text              string    `json:"text"`
		Status            string    `json:"status"`
		AffectedLocations []struct {
			Title string `json:"title"`
			ID    string `json:"id"`
		} `json:"affected_locations"`
	} `json:"updates"`
	MostRecentUpdate struct {
		Created           time.Time `json:"created"`
		Modified          time.Time `json:"modified"`
		When              time.Time `json:"when"`
		Text              string    `json:"text"`
		Status            string    `json:"status"`
		AffectedLocations []struct {
			Title string `json:"title"`
			ID    string `json:"id"`
		} `json:"affected_locations"`
	} `json:"most_recent_update"`
	StatusImpact     string `json:"status_impact"`
	Severity         string `json:"severity"`
	ServiceKey       string `json:"service_key"`
	ServiceName      string `json:"service_name"`
	AffectedProducts []struct {
		Title string `json:"title"`
		ID    string `json:"id"`
	} `json:"affected_products"`
	URI                         string        `json:"uri"`
	CurrentlyAffectedLocations  []interface{} `json:"currently_affected_locations"`
	PreviouslyAffectedLocations []struct {
		Title string `json:"title"`
		ID    string `json:"id"`
	} `json:"previously_affected_locations"`
}

const (
	url = "https://status.cloud.google.com/incidents.json"
)

var (
	bqDataset   = flag.String("bqDataset", "status_dataset", "BigQuery Dataset to write to")
	bqTable     = flag.String("bqTable", "status", "BigQuery Table to write to")
	bqProjectID = flag.String("bqProjectID", os.Getenv("BQ_PROJECTID"), "Project for the BigQuery Dataset to write to")
)

func fronthandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("/ called \n")

	var events []Event

	client := http.Client{
		Timeout: time.Second * 2,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Printf("Error getting status JSON %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error getting status JSON Request %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&events)
	if err != nil {
		fmt.Printf("Error parsing status JSON %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var rlines []string
	for _, event := range events {
		strEvent, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("Error Marshal Event %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rlines = append(rlines, string(strEvent))
	}

	dataString := strings.Join(rlines, "\n")
	rolesSource := bigquery.NewReaderSource(strings.NewReader(dataString))

	rolesSource.SourceFormat = bigquery.JSON

	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, *bqProjectID)
	if err != nil {
		fmt.Printf("Error Creating BQ Client %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ds := bqClient.Dataset(*bqDataset)

	rolesTable := ds.Table(*bqTable)
	rloader := rolesTable.LoaderFrom(rolesSource)
	rloader.CreateDisposition = bigquery.CreateNever

	rjob, err := rloader.Run(ctx)
	if err != nil {
		fmt.Printf("Error creating loader BQ with JobID %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rstatus, err := rjob.Wait(ctx)
	if err != nil {
		fmt.Printf("Error loading data Wait to BQ jobID [%s]  %v\n", rjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := rstatus.Err(); err != nil {
		fmt.Printf("Error Loading Data Status:\n %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("  Done  %v\n", rstatus.Statistics)

}

func main() {

	http.HandleFunc("/", fronthandler)

	server := &http.Server{
		Addr: ":8080",
	}
	http2.ConfigureServer(server, &http2.Server{})
	fmt.Println("Starting Server..")
	err := server.ListenAndServe()
	fmt.Printf("Unable to start Server %v", err)
}
