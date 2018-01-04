package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"projects/goTeamTIM/CDN/elaboralog"
	"projects/goTeamTIM/elasticTIM"
	"runtime"
	"sync"
)

var (
	Listalog = "cdnrecords"
    Records := make(chan []string)
	wg sync.WaitGroup
)

func main() {

	//trace.Start(os.Stdout)
	//defer trace.Stop()

	runtime.GOMAXPROCS(runtime.NumCPU()) //esegue una go routine su tutti i processori

	dat, _ := ioutil.ReadFile("mapping.json")
	mapping := string(dat)
	wg.Add(1)
	go elasticTIM.IngestaInElastic2("http://127.0.0.1:9200", "cdn", "log", Listalog, mapping, Records)

	for _, file := range os.Args[1:] {
		fmt.Println(file)
		wg.Add(1)
		go elaboralog.Leggizip(file, &wg) -> Records
	}
	wg.Wait()
	
	return
}
