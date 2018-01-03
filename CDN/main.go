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

	wg sync.WaitGroup
)

func main() {

	//trace.Start(os.Stdout)
	//defer trace.Stop()

	runtime.GOMAXPROCS(runtime.NumCPU()) //esegue una go routine su tutti i processori

	for _, file := range os.Args[1:] {
		fmt.Println(file)
		wg.Add(1)
		go elaboralog.Leggizip(file, &wg)
	}
	wg.Wait()
	dat, _ := ioutil.ReadFile("mapping.json")
	mapping := string(dat)
	elasticTIM.IngestaInElastic("mammolo-logginator.westeurope.cloudapp.azure.com:9200", "cdn", "log", Listalog, mapping)

	return
}
