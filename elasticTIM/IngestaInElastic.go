package elasticTIM

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis"
	"github.com/olivere/elastic"
)

var (
	redispwd = os.Getenv("REDIS_PWD")
	//Creazione client Redis
	clientR = redis.NewClient(&redis.Options{ //connettiti a Redis server
		Addr:     "localhost:6379",
		Password: redispwd, // no password set
		DB:       0,        // use default DB
	})
)

var wgg sync.WaitGroup

//IngestaInElastic preleva dalla "lista" in redis gli elementi da ingestare in Elasticsearch secondo il "mapping" fornito
//"elastichost" è l'host di elasticsearch da usare, "index" è l'index di destinazione (se non esiste viene creato) e il "tipo" deve essere in regola col mapping
func IngestaInElastic(elastichost, index, tipo, lista, mapping string) {
/* 
	//Verifica che Redis risponda
	_, errore := clientR.Ping().Result()
	if errore != nil {
		fmt.Println("Qualcuno ha spento Redis? O sbagli passord? Essere umano che leggi...RIPARA!")
		fmt.Println("la pwd per redis va impostata così: export REDIS_PWD=***")
		os.Exit(1)
	} */

	ctx := context.Background()

	//Istanzia client per Elasticsearch
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
		createIndex, err := client.CreateIndex(index).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	//Creazione client Elasticsearch per inserimenti massivi
	cb := elastic.NewBulkService(client)
	if err != nil {
		panic(err)
	}
	//Sembrerebbe dover ciclare all'infinito ma ci sono i break nel loop
	for {
		//numero di record nella lista di redis
		numrecords, _ := clientR.LLen(lista).Result()
		//trasformazione da int64 a int
		num := int(numrecords)

		//stampa a video il numero di item nella lista di redis
		fmt.Println(num)

		//facciamo dei mazzi
		mazzo := 5000   //Quanti record da gestire insieme
		numroutine := 5 //Quante Go routine usare

		//Dividiamo il carico dell'ingestamento su più go routine
		//per dare un minimo di informazioni su Kibana a ogni refresh
		if num > mazzo {
			for n := 0; n < numroutine; n++ {
				wgg.Add(1) //aggiunge uno al syncgroup
				go func() {
					defer wgg.Done()
					for i := 0; i < (mazzo / numroutine); i++ {

						recordjson, _ := clientR.RPop(lista).Result()

						//fmt.Println(recordjson) //scommentare per troubleshoting
						hasher := md5.New()                         //prepara a fare un hash
						hasher.Write([]byte(recordjson))            //hasha tutta la linea
						Hash := hex.EncodeToString(hasher.Sum(nil)) //estrae l'hash md5sum in versione quasi human readable
						//Hash fungerà da indice del record in Elasticsearch, quindi si evitato i doppi inserimenti
						req := elastic.NewBulkIndexRequest().Index(index).Type(tipo).Id(Hash).Doc(recordjson)
						cb.Add(req)
					}
					//per scrivere effetticamente il mazzo su elasticsearch
					_, err = cb.Do(ctx)
					if err != nil {
						fmt.Println(err)
					}
					return //fine della go rountine
				}()
				fmt.Println("Avviata go routine: ", n) //scommentare per troubleshoting
			}
			wgg.Wait() //Attende che finiscano tutte le goroutine
		}
		//se gli elementi in lista sono meno di mazzo si gestiscono uno per volta
		if num <= mazzo {
			for i := 0; i < num; i++ {
				recordjson, _ := clientR.RPop(lista).Result()

				//fmt.Println(recordjson)
				hasher := md5.New()                         //prepara a fare un hash
				hasher.Write([]byte(recordjson))            //hasha tutta la linea
				Hash := hex.EncodeToString(hasher.Sum(nil)) //estrae l'hash md5sum in versione quasi human readable
				//Hash fungerà da indice del record in Elasticsearch, quindi si evitato i doppi inserimenti
				req := elastic.NewBulkIndexRequest().Index(index).Type(tipo).Id(Hash).Doc(recordjson)
				cb.Add(req)
			}
			_, err = cb.Do(ctx)
			if err != nil {
				fmt.Println(err)
			}
			//il break è importante se no si alluuuupaaaaa tutto
			break
		}
		continue
	}
	return
}

func IngestaInElastic2(elastichost, index, tipo, lista, mapping string, records chan string) {

	
		ctx := context.Background()
	
		//Istanzia client per Elasticsearch
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
			createIndex, err := client.CreateIndex(index).BodyString(mapping).Do(ctx)
			if err != nil {
				// Handle error
				panic(err)
			}
			if !createIndex.Acknowledged {
				// Not acknowledged
			}
		}
	
		//Creazione client Elasticsearch per inserimenti massivi
		cb := elastic.NewBulkService(client)
		if err != nil {
			panic(err)
		}
		//Sembrerebbe dover ciclare all'infinito ma ci sono i break nel loop
		for recordjson := range Records {
	
							//fmt.Println(recordjson) //scommentare per troubleshoting
							hasher := md5.New()                         //prepara a fare un hash
							hasher.Write([]byte(recordjson))            //hasha tutta la linea
							Hash := hex.EncodeToString(hasher.Sum(nil)) //estrae l'hash md5sum in versione quasi human readable
							//Hash fungerà da indice del record in Elasticsearch, quindi si evitato i doppi inserimenti
							req := elastic.NewBulkIndexRequest().Index(index).Type(tipo).Id(Hash).Doc(recordjson)
							cb.Add(req)
						}
						//per scrivere effetticamente il mazzo su elasticsearch
						_, err = cb.Do(ctx)
						if err != nil {
							fmt.Println(err)
						}
						return //fine della go rountine

					fmt.Println("Avviata go routine: ", n) //scommentare per troubleshoting
				}
				wgg.Wait() //Attende che finiscano tutte le goroutine
			}
			//se gli elementi in lista sono meno di mazzo si gestiscono uno per volta
			if num <= mazzo {
				for i := 0; i < num; i++ {
					recordjson <- Records
					//fmt.Println(recordjson)
					hasher := md5.New()                         //prepara a fare un hash
					hasher.Write([]byte(recordjson))            //hasha tutta la linea
					Hash := hex.EncodeToString(hasher.Sum(nil)) //estrae l'hash md5sum in versione quasi human readable
					//Hash fungerà da indice del record in Elasticsearch, quindi si evitato i doppi inserimenti
					req := elastic.NewBulkIndexRequest().Index(index).Type(tipo).Id(Hash).Doc(recordjson)
					cb.Add(req)
				}
				_, err = cb.Do(ctx)
				if err != nil {
					fmt.Println(err)
				}
				//il break è importante se no si alluuuupaaaaa tutto
				break
			}
			continue
		}
		return
	}

func main() {
	for {
		res := clientR.BRPop(0,"cdnrecords").Result()
		res[1]
		//IngestaInElastic("http://127.0.0.1:9200", "cdn", "log", , mapping string
	}
}