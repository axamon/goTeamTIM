package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/go-redis/redis"
)

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

func main() {
	ipclient := os.Args[1]
	testInput := net.ParseIP(ipclient)
	if testInput.To4() == nil {
		fmt.Printf("%v non è un IP address v4 valido\n", ipclient)
		os.Exit(200)
	}
	if testInput.IsGlobalUnicast() == false {
		fmt.Printf("%v non è un ipv4 pubblico\n", ipclient)
		os.Exit(201)
	}
	if testInput.IsLoopback() == true {
		fmt.Printf("%v è un ipv4 di loopback\n", ipclient)
		os.Exit(202)
	}
	date := "20171115"
	res, err := RedisClient.SMembers(date + "_" + ipclient).Result()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)
}
