package main

import (
	"cloud.google.com/go/bigquery"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	apiv3 "cloud.google.com/go/translate/apiv3"
	"cloud.google.com/go/translate/apiv3/translatepb"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"log"
	"net/http"
	"os"
	"sync"
)

var projectID = "zatar-demo"

// spanner grpc client
var adminClient *database.DatabaseAdminClient

// bigquery http client
var bqClient *bigquery.Client

// translate grpc client
var translateGrpcClient *apiv3.TranslationClient

// translate http client
var translateHttpClient *apiv3.TranslationClient
var ctx context.Context

func main() {
	var err error

	ctx = context.Background()

	// spanner admin grpc client
	adminClient, err = database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot start spanner admin grpc client")
	}
	defer adminClient.Close()

	// bigquery http client
	bqClient, err = bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("can not create bigquery http client: %v", err)
	}
	defer bqClient.Close()

	// translate grpc client with connection pool size = 5
	translateGrpcClient, err = apiv3.NewTranslationClient(ctx, option.WithGRPCConnectionPool(5))
	if err != nil {
		log.Fatalf("can not create translate grpc client: %v", err)
	}
	defer translateGrpcClient.Close()

	// translate http client
	translateHttpClient, err = apiv3.NewTranslationRESTClient(ctx)
	if err != nil {
		log.Fatalf("can not create translate http client: %v", err)
	}
	defer translateHttpClient.Close()

	http.HandleFunc("/", indexHandler)
	// lists database in our test spanner instance
	http.HandleFunc("/spannergrpc", spannerGrpcHandler)

	// runs a simple query and return the results
	http.HandleFunc("/bigqueryhttp", bigqueryHttpHandler)

	// concurrently translates sentences, using the grpc client
	http.HandleFunc("/translatehttp", translateHttpHandler)

	// concurrently translates sentences, using the http client
	http.HandleFunc("/translategrpc", translateGrpcHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("Hello and Welcome!"))
	return
}

func spannerGrpcHandler(w http.ResponseWriter, r *http.Request) {
	instanceId := "projects/zatar-demo/instances/test-instance"
	iter := adminClient.ListDatabases(ctx, &adminpb.ListDatabasesRequest{
		Parent: instanceId,
	})
	fmt.Fprintf(w, "Databases for instance/[%s]\n", instanceId)
	for {
		resp, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("error iterating through response: %v", err)))
			return
		}
		fmt.Fprintf(w, "%s\n", resp.Name)
	}

	w.Write([]byte("listing database was successful!"))
	return
}

func bigqueryHttpHandler(w http.ResponseWriter, r *http.Request) {
	q := "-- This query shows a list of the daily top Google Search terms.\n" +
		"SELECT\n   " +
		"refresh_date AS Day,\n   " +
		"term AS Top_Term,\n       " +
		"-- These search terms are in the top 25 in the US each day.\n " +
		"rank,\n" +
		"FROM `bigquery-public-data.google_trends.top_terms`\n" +
		"WHERE  rank = 1\n       " +
		"-- Choose only the top term each day.\n " +
		"AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)\n       " +
		"-- Filter to the last 1 weeks. \n" +
		"GROUP BY Day, Top_Term, rank \n" +
		"ORDER BY Day DESC"

	query := bqClient.Query(q)

	// Location must match that of the dataset(s) referenced in the query.
	query.Location = "US"

	// Run the query and print results when the query job is completed.
	job, err := query.Run(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("can not run query: %v", err)))
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("error waiting on job to finish: %v", err)))
		return
	}
	if err := status.Err(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("error in job status: %v", err)))
		return
	}
	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("error iterating results: %v", err)))
			return
		}
		fmt.Fprintln(w, row)
	}

	log.Printf("query is done!")
	return
}

func translateGrpcHandler(w http.ResponseWriter, r *http.Request) {

	var wg sync.WaitGroup
	texts := []string{
		"s2a is awesome",
		"zatar is great",
		"authentication is important",
		"mtls is a must",
		"google cloud is better than aws",
		"google cloud is better than azure",
		"how are you?",
		"good morning",
		"summer is the best",
		"I love Sunnyvale",
	}
	wg.Add(len(texts))
	for i := 0; i < len(texts); i++ {
		go func(t string) {
			dotranslate(t, "zh", ctx, w, translateGrpcClient)
			wg.Done()
		}(texts[i])
	}
	wg.Wait()
}

func translateHttpHandler(w http.ResponseWriter, r *http.Request) {
	var wg sync.WaitGroup
	texts := []string{
		"s2a is awesome",
		"zatar is great",
		"authentication is important",
		"mtls is a must",
		"google cloud is better than aws",
		"google cloud is better than azure",
		"how are you?",
		"good morning",
		"summer is the best",
		"I love Sunnyvale",
	}
	wg.Add(len(texts))
	for i := 0; i < len(texts); i++ {
		go func(t string) {
			dotranslate(t, "ar", ctx, w, translateHttpClient)
			wg.Done()
		}(texts[i])
	}
	wg.Wait()
}

func dotranslate(text string, targetLangCode string, ctx context.Context, w http.ResponseWriter, c *apiv3.TranslationClient) {
	log.Printf("sending translate text request ...")
	req := &translatepb.TranslateTextRequest{
		Parent:             fmt.Sprintf("projects/%s/locations/%s", "zatar-demo", "us-central1"),
		TargetLanguageCode: targetLangCode,
		Contents:           []string{text},
		// See https://pkg.go.dev/cloud.google.com/go/translate/apiv3/translatepb#TranslateTextRequest.
	}
	resp, err := c.TranslateText(ctx, req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Translate: %v", err)))
		return
	}
	for _, translation := range resp.GetTranslations() {
		fmt.Fprintf(w, "Translated text: %v\n", translation.GetTranslatedText())
	}
}
