package db

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// reddis need context
var RDB *redis.Client

// background conetxt
var Ctx = context.Background()

func ConnectRedis() {
	RDB = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	//ping to reddis

	_, err := RDB.Ping(Ctx).Result()
	if err != nil {
		log.Fatal("Reddis connectionm failed", err)
	}
	log.Println("Connected to Redis")
}
