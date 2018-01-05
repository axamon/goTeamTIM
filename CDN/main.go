package main

import (
	"fmt"
	"os"
	"projects/goTeamTIM/CDN/elaboralog"
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
		go elaboralog.Leggizip2(file, &wg)
	}
	wg.Wait()
	return
}
