package elaboralog

import (
	"context"
	"io/ioutil"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/klauspost/pgzip"
	"github.com/olivere/elastic"
	//"compress/gzip"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"strings"
)

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
	Type        string
	Time        string
	TTS         int
	SEIp        string
	Clientip    string
	Request     string
	TCPStatus   string
	HTTPStatus  int
	Bytes       int
	Speed       float32
	Method      string
	URL         string
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
func Leggizip(elastichost, file string, wg *sync.WaitGroup, status int) {
	ctx := context.Background()
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
	data := fileelements[4]

	index := "we_accesslog_" + data
	creaindice(elastichost, index)
	//index := "errori"
	fmt.Println("index: ", index, Type)

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
		dat, _ := ioutil.ReadFile("mapping.json")
		mapping := string(dat)
		fmt.Println(mapping)
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

	p, err := client.BulkProcessor().
		Name("MyBackgroundWorker-1").
		Workers(4).
		BulkActions(1000).               // commit if # requests >= 1000
		BulkSize(2 << 20).               // commit if size of requests >= 2 MB
		FlushInterval(10 * time.Second). // commit every 30s
		Do(ctx)

	if err != nil {
		os.Exit(7000)
	}

	fmt.Println("inizio scanner")
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
		Type = "accesslog" //se il tipo di log è "accesslog"
		TTS, _ := strconv.Atoi(s[1])
		Clientip := s[2]
		Request := s[3]
		elements := strings.Split(Request, "/")
		TCPStatus := elements[0]
		HTTPStatus, _ := strconv.Atoi(elements[1])
		//fmt.Println(HTTPStatus)
		if HTTPStatus < status { // status è un int passato dalla main function
			continue
		}
		Bytes, _ := strconv.Atoi(s[4])
		Speed := float32(Bytes / TTS)
		Method := s[5]
		URL := s[6]
		Urlschema := u.Scheme
		Urlhost := u.Host
		if strings.Contains(Urlhost, ".se.") == true {
			Urlhost = strings.Split(Urlhost, ".se.")[1]
		}
		Urlpath := u.Path
		Urlquery := u.RawQuery
		Urlfragment := u.Fragment
		//gestione url finita
		Mime := s[7]
		Ua := s[8]
		//Ui := Clientip + Ua

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
			URL:         URL,
			Urlschema:   Urlschema,
			Urlhost:     Urlhost,
			Urlpath:     Urlpath,
			Urlquery:    Urlquery,
			Urlfragment: Urlfragment,
			Mime:        Mime,
			Ua:          Ua}

		elerecord2, _ := json.Marshal(elerecord)
		recordjson := string(elerecord2)
		//fmt.Println(recordjson)
		/* hasher := md5.New()                         //prepara a fare un hash
		hasher.Write([]byte(recordjson))            //hasha tutta la linea
		Hash := hex.EncodeToString(hasher.Sum(nil)) //estrae l'hash md5sum in versione quasi human readable
		//Hash fungerà da indice del record in Elasticsearch, quindi si evitato i doppi inserimenti */
		tipo := "accesslog"
		idrecord, _ := uuid.NewUUID()
		iddrecord := idrecord.String()
		req := elastic.NewBulkIndexRequest().Index(index).Type(tipo).Id(iddrecord).Doc(recordjson)
		p.Add(req)
	}

	err = p.Flush()
	if err != nil {
		//os.Exit(700)
		fmt.Println(err)
	}
	fmt.Println("ingestato: ", file)

	return //terminata la Go routine!!! :)
}
