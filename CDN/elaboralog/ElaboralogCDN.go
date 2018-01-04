package elaboralog

import (
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/klauspost/pgzip"
	//"compress/gzip"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"strings"
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

//Log permette l'unmarshalling dei file json
type Log struct {
	Type         string `json:"type"`
	Time         string `json:"time"`
	TTS          int    `json:"tts"`
	SEIp         string `json:"seip"`
	Clientip     string `json:"clientip"`
	Request      string `json:"req"`
	Bytes        int    `json:"bytes"`
	Method       string `json:"method"`
	Urlschema    string `json:"urlschema"`
	Urlhost      string `json:"urlhost"`
	Urlpath      string `json:"urlpath"`
	Urlquery     string `json:"urlquery"`
	Urlfragment  string `json:"urlfragment"`
	Mime         string `json:"mime"`
	Ua           string `json:"ua"`
	ServerIP     string `json:"serverip,omitempty"`
	BytesRead    int    `json:"bytesread,omitempty"`
	BytesToRead  int    `json:"bytestoread,omitempty"`
	AssetSize    int    `json:"assetsize,omitempty"`
	Status       string `json:"status,omitempty"`
	IngestStatus string `json:"ingeststatus,omitempty"`
}

//Accesslog è il Template dei log Transaction
type Accesslog struct {
	//Hash        string
	Type       string
	Time       string
	TTS        int
	SEIp       string
	Clientip   string
	Request    string
	TCPStatus  string
	HTTPStatus string
	Bytes      int
	Speed      float32
	Method     string
	//Url         string
	Urlschema   string
	Urlhost     string
	Urlpath     string
	Urlquery    string
	Urlfragment string
	Mime        string
	Ua          string
}

//Ingestlogtest è il Template dei log ingestion
type Ingestlogtest struct {
	Type string
	//Hash         string
	Time string
	//URL          string
	SEIp         string
	Urlschema    string
	Urlhost      string
	Urlpath      string
	Urlquery     string
	Urlfragment  string
	ServerIP     string
	BytesRead    int
	BytesToRead  int
	AssetSize    int
	Status       string
	IngestStatus string
}

//Ingestlog è il template dei log ingestion
type Ingestlog struct {
	Type string
	//Hash             string
	Time string
	//URL              string
	SEIp             string
	Urlschema        string
	Urlhost          string
	Urlpath          string
	Urlquery         string
	Urlfragment      string
	FailOverSvrList  string
	ServerIP         string
	BytesRead        int
	BytesToRead      int
	AssetSize        int
	DownloadComplete string
	DownloadTime     string
	ReadCallBack     string
	Status           string
	Mime             string
	Revaldidation    string
	CDSDomain        string
	ConnectionInfo   string
	IngestStatus     string
	RedirectedURL    string
	OSFailoverAction string
	BillingCookie    string
}

// var wg = sizedwaitgroup.New(250) //massimo numero di go routine per volta

//Leggizip riceve come argomento un file CDN zippato e lo processa
func Leggizip(file string, wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	// gr, err := gzip.NewReader(f)
	gr, err := pgzip.NewReaderN(f, 1024, 2048) //sfrutta il gzip con steroidi che legge nel futuro per andare più veloce e meglio assai del nomale gzip

	if err != nil { //se però si impippa qualcosa allora blocca tutto
		log.Fatal(err)
		os.Exit(1)
	}

	fileelements := strings.Split(file, "_") //prende il nome del file di log e recupera i campi utili
	Type := fileelements[1]                  //qui prede il tipo di log
	SEIp := fileelements[3]                  //qui prende l'ip della cache
	elenco := make([]string, 0, 1000)

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
				return
			}
			t, err := time.Parse("[02/Jan/2006:15:04:05.000-0700]", s[0]) //converte i timestamp come piacciono a me
			if err != nil {
				fmt.Println(err)
			}
			Time := t.Format("2006-01-02T15:04:05.000Z") //idem con patate questo è lo stracazzuto ISO8601 meglio c'è solo epoch
			// fmt.Println(Time)
			u, err := url.Parse(s[6]) //prendi una URL, trattala male, falla a pezzi per ore...
			if err != nil {
				log.Fatal(err)
			}
			TTS, _ := strconv.Atoi(s[1])
			Clientip := s[2]
			Request := s[3]
			elements := strings.Split(Request, "/")
			TCPStatus := elements[0]
			HTTPStatus := elements[1]
			Bytes, _ := strconv.Atoi(s[4])
			Speed := float32(Bytes / TTS)
			Method := s[5]
			//Url := s[6]
			Urlschema := u.Scheme
			Urlhost := u.Host
			Urlpath := u.Path
			Urlquery := u.RawQuery
			Urlfragment := u.Fragment
			//gestione url finita
			Mime := s[7]
			Ua := s[8]

			elerecord := Accesslog{
				Type:        Type,
				Time:        Time,
				TTS:         TTS,
				SEIp:        SEIp,
				Clientip:    Clientip,
				Request:     Request,
				TCPStatus:   TCPStatus,
				HTTPStatus:  HTTPStatus,
				Bytes:       Bytes,
				Speed:       Speed,
				Method:      Method,
				Urlschema:   Urlschema,
				Urlhost:     Urlhost,
				Urlpath:     Urlpath,
				Urlquery:    Urlquery,
				Urlfragment: Urlfragment,
				Mime:        Mime,
				Ua:          Ua}

			elerecord2, _ := json.Marshal(elerecord)
			elerecord3 := string(elerecord2)

			elenco = append(elenco, elerecord3) //mettiamo tutto in una slice
			fmt.Println(len(elenco), cap(elenco))
			if len(elenco) >= 1000 { //se la slice supera il limite imposto
				pipe := RedisClient.Pipeline() // si attiva una pipeline su redis
				for _, item := range elenco {  // uno per uno si scarica la slice
					err := pipe.LPush(Listalog, item).Err() //e la si mette nel pipe
					if err != nil {                         // se ci sono errori blocca tutto
						fmt.Println(err)
						os.Exit(400)
					}
				}
				_, err := pipe.Exec() //esegui la pipeline
				//fmt.Println(val)        //mosta a video sto coso
				if err != nil { //se ci sono errori stoppa tutto
					fmt.Println(err)
					os.Exit(401)
				}
				elenco = make([]string, 0, 1000) //ripulisce la slice
			}
		}
	}

	if Type == "ingestlog" {
		scan := bufio.NewScanner(gr) //mettiamo tutto in un buffer che è rapido

		var saltariga int //per saltare le prime righe inutili
		for scan.Scan() {
			if saltariga < 2 { //salta le prime due righe
				scan.Text()
				saltariga = saltariga + 1
				continue
			}
			line := scan.Text()

			s := strings.Split(line, " ") //splitta le linee secondo il delimitatore usato nel file di log, cambiare all'occorrenza

			if len(s) < 20 { // se i parametri sono meno di 20 allora ricomincia il loop, serve a evitare le linee che non ci interessano
				return
			}

			t, err := time.Parse("[02/Jan/2006:15:04:05.000-0700]", s[0]) //quant'è bello parsare i timestamp in go :)
			if err != nil {
				fmt.Println(err)
			}
			Time := t.Format("2006-01-02T15:04:05.000Z") //ISO8601 mon amour

			//gestiamo le url secondo l'RFC ... non mi ricordo qual è
			u, err := url.Parse(s[1]) //prendi una URL, trattala male, falla a pezzi per ore...
			if err != nil {
				log.Fatal(err)
			}
			//URL := s[1]
			Urlschema := u.Scheme
			Urlhost := u.Host
			Urlpath := u.Path
			Urlquery := u.RawQuery
			Urlfragment := u.Fragment
			ServerIP := s[3]
			BytesRead, _ := strconv.Atoi(s[4])   //trasforma il valore in int
			BytesToRead, _ := strconv.Atoi(s[5]) //trasforma il valore in int
			AssetSize, _ := strconv.Atoi(s[6])   //trasforma il valore in int
			Status := s[10]
			IngestStatus := s[15]

			elerecord := Log{
				Time:         Time,
				SEIp:         SEIp,
				Urlschema:    Urlschema,
				Urlhost:      Urlhost,
				Urlpath:      Urlpath,
				Urlquery:     Urlquery,
				Urlfragment:  Urlfragment,
				ServerIP:     ServerIP,
				BytesRead:    BytesRead,
				BytesToRead:  BytesToRead,
				AssetSize:    AssetSize,
				Status:       Status,
				IngestStatus: IngestStatus}
			fmt.Println(elerecord)

		}
		//fmt.Printf("%+v\n", l)

	}

	return //terminata la Go routine!!! :)
}
