package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/go-redis/redis"
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

func init() {
	//Verifica che Redis risponda
	_, errore := clientR.Ping().Result()
	if errore != nil {
		fmt.Println("Qualcuno ha spento Redis? O sbagli passord? Essere umano che leggi...RIPARA!")
		fmt.Println("la pwd per redis va impostata così: export REDIS_PWD=***")
		os.Exit(400)
	}
}

func main() {
	files, err := ioutil.ReadDir(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		logfile := file.Name()
		res, _ := clientR.SAdd("setlogfiles", logfile).Result()
		if res == 1 {
			_, err := clientR.LPush("listacdnlogfiles", logfile).Result()
			if err != nil {
				os.Exit(250)
			}
		}
		//fmt.Println(file.Name())
	}
	return
}
