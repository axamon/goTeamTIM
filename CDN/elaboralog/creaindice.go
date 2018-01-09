package elaboralog

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/olivere/elastic"
)

//var elastichost = "http://127.0.0.1:9200"

//var index = "we_accesslog_20171115"

func creaindice(elastichost, index string) {
	ctx := context.Background()
	client, err := elastic.NewClient(elastic.SetURL(elastichost))
	if err != nil {
		fmt.Println(err)
		os.Exit(401)
	}
	//.elastic.SetBasicAuth("user", "secret"))

	_, _, errela := client.Ping(elastichost).Do(ctx)
	if errela != nil {
		// Handle error
		panic(errela)
	}
	//fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(index).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		dat, _ := ioutil.ReadFile("mapping.json")
		mapping := string(dat)
		//fmt.Println(mapping)
		createIndex, err := client.CreateIndex(index).BodyString(mapping).Do(ctx)

		time.Sleep(1 * time.Second)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			panic(err)
		}
		fmt.Println("creato indice: ", index)
	}
}
