package main

import (
	"flag"
	"fmt"
	"os"
	"projects/goTeamTIM/CDN/elaboralog"
	"runtime"
	"strings"
	"sync"
)

var (
	//Redispwd è la password per accedere a redis va impostata nell'ambiente dello user con variabile REDIS_PWD
	/* 	Redispwd = os.Getenv("REDIS_PWD")

	   	//RedisClient è il client Redis utilizabile
	   	RedisClient = redis.NewClient(&redis.Options{ //connettiti a Redis server
	   		Addr:     "localhost:6379",
	   		Password: Redispwd, // no password set
	   		DB:       0,        // use default DB
	   	}) */

	//Sync group
	wg sync.WaitGroup

	//Listalog è il nome della lista in redis dove accodare i record json
	Listalog = "cdnrecords"
)

/* func init() {
	//Verifica che Redis risponda
	_, errore := RedisClient.Ping().Result()
	if errore != nil {
		fmt.Println("Qualcuno ha spento Redis? O sbagli passord? Essere umano che leggi...RIPARA!")
		fmt.Println("la pwd per redis va impostata così: export REDIS_PWD=***")
		//os.Exit(1)
	}
} */

func main() {

	//trace.Start(os.Stdout)
	//defer trace.Stop()

	runtime.GOMAXPROCS(runtime.NumCPU()) //esegue una go routine su tutti i processori

	status := flag.Int("status", 100, "minimo http status da ingestare, se metti 400 prenderà gli status da 400 in su")
	elastichost := flag.String("elastichost", "http://127.0.0.1:9200", "host dove contattare elasticsearch")
	flag.Parse()

	for _, file := range os.Args[1:] {
		if strings.HasPrefix(file, "--") {
			continue
		}
		fmt.Println(file)
		wg.Add(1)
		go elaboralog.Leggizip(*elastichost, file, &wg, *status)
		wg.Wait()
	}
	return
}
