package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/klauspost/pgzip"
)

//Accesslog è il Template dei log Transaction
type Accesslog struct {
	//Hash        string
	Type     string
	Time     string
	TTS      int
	SEIp     string
	Clientip string
	Request  string
	Bytes    int
	Method   string
	//Url         string
	Urlschema   string
	Urlhost     string
	Urlpath     string
	Urlquery    string
	Urlfragment string
	Mime        string
	Ua          string
}

var (
	//Redispwd è la password per redis
	Redispwd = os.Getenv("REDIS_PWD")

	//RedisClient è il  client Redis
	RedisClient = redis.NewClient(&redis.Options{ //connettiti a Redis server
		Addr:     "localhost:6379",
		Password: Redispwd, // no password set
		DB:       0,        // use default DB
	})

	wg sync.WaitGroup
)

//IP2File estrae gli ip pubblici dei client e vi associa tutti i file zippati dove sono presenti in una chiave su Redis
func IP2File(file string, wg *sync.WaitGroup) {
	defer wg.Done()

	/* var wgr sync.WaitGroup */
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	// gr, err := gzip.NewReader(f)
	gr, err := pgzip.NewReaderN(f, 1024, 2048) //sfrutta il gzip con steroide che legge nel futuro per andare più veloce assai

	if err != nil { //se però si impippa qualcosa allora blocca tutto
		log.Fatal(err)
		os.Exit(1)
	}

	basefile := filepath.Base(file)

	fileelements := strings.Split(file, "_") //prende il nome del file di log e recupera i campi utili
	Type := fileelements[1]                  //qui prede il tipo di log
	//SEIp := fileelements[3]                  //qui prende l'ip della cache
	data := fileelements[4] //qui prende la data(
	pipe := RedisClient.Pipeline()
	n := 0
	if Type == "accesslog" { //se il tipo di log è "accesslog"
		scan := bufio.NewScanner(gr)
		var saltariga int //per saltare le prime righe inutili
		for scan.Scan() {
			if saltariga < 2 { //salta le prime due righe
				scan.Text()
				saltariga = saltariga + 1
				continue
			}
			line := scan.Text()

			s := strings.Split(line, "\t")
			if len(s) < 5 { // se i parametri sono meno di 20 allora ricomincia il loop, serve a evitare le linee che non ci interessano
				continue
			}

			Clientip := s[2]

			key := data + "_" + Clientip

			err := pipe.SAdd(key, basefile).Err()
			if err != nil {
				fmt.Println(err)
				os.Exit(200)
			}
			t10g := (10 * 24 * time.Hour)
			pipe.Expire(key, t10g) //la chiave spira dopo 10 gg
			n++
			if n >= 100 {
				_, err := pipe.Exec()
				if err != nil {
					fmt.Println(err)
					os.Exit(200)
				}
			}
		}
		//RedisClient.Pipeline().Close()

		_, err := pipe.Exec()
		if err != nil {
			fmt.Println(err)
			os.Exit(200)
		}
	}

	if Type == "ingestlog" {
		return
	}

	return //terminata la Go routine!!! :)
}

func main() {
	n := 0
	for _, file := range os.Args[1:] {
		fmt.Println(file)
		n++
		wg.Add(1)
		go IP2File(file, &wg)
		if n >= 10 {
			wg.Wait()
			n = 0
		}
	}

	return
}
