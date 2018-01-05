package main

import (
	"fmt"
	"os"
	"projects/goTeamTIM/CDN/elaboralog"
	"runtime"
	"sync"

	"github.com/go-redis/redis"
)

var (
	//Redispwd è la password per accedere a redis va impostata nell'ambiente dello user con variabile REDIS_PWD
	Redispwd = os.Getenv("REDIS_PWD")

	//RedisClient è il client Redis utilizabile
	RedisClient = redis.NewClient(&redis.Options{ //connettiti a Redis server
		Addr:     "localhost:6379",
		Password: Redispwd, // no password set
		DB:       0,        // use default DB
	})

	//Sync group
	wg sync.WaitGroup

	//Listalog è il nome della lista in redis dove accodare i record json
	Listalog = "cdnrecords"
)

func init() {
	//Verifica che Redis risponda
	_, errore := RedisClient.Ping().Result()
	if errore != nil {
		fmt.Println("Qualcuno ha spento Redis? O sbagli passord? Essere umano che leggi...RIPARA!")
		fmt.Println("la pwd per redis va impostata così: export REDIS_PWD=***")
		os.Exit(1)
	}
}

func main() {

	//trace.Start(os.Stdout)
	//defer trace.Stop()

	runtime.GOMAXPROCS(runtime.NumCPU()) //esegue una go routine su tutti i processori

	for {
		res := RedisClient.BRPop(10, os.Args[1]).Val()
		/* if err != nil {
			fmt.Println(err)
			os.Exit(500)
		} */
		file := res[1]
		fmt.Println(file)
		wg.Add(1)
		go elaboralog.Leggizip(file, &wg)
		wg.Wait()
	}
	return
}
