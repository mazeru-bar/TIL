package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

func getRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		// docker-composeで起動したRedisのホスト名を指定
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return client
}

func redisLock(ctx context.Context, client *redis.Client, key string) error {
	const maxRetries = 5

	txf := func(tx *redis.Tx) error {
		// SETのKEEPTTLオプションのような挙動を再現する
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to get TTL: %v", err)
		}
		log.Printf("TTL before pipeline: %v", ttl)

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			_, err = pipe.Set(ctx, key, "new value", ttl).Result()
			if err != nil {
				return fmt.Errorf("failed to set key: %v", err)
			}
			_, err = pipe.Expire(ctx, key, ttl).Result()
			if err != nil {
				return fmt.Errorf("failed to set TTL: %v", err)
			}
			return nil
		})

		return err
	}

	for i := 0; i < maxRetries; i++ {
		err := client.Watch(ctx, txf, key)
		// トランザクションに成功した場合
		if err == nil {
			return nil
		}
		// ロックの取得に失敗した場合
		if err == redis.TxFailedErr {
			log.Printf("Optimistic lock lost. Retrying... (count=%d)", i+1)
			continue
		}
		// その他のエラーで失敗した場合
		return err
	}

	return nil
}

func main() {
	client := getRedisClient()
	var ctx = context.Background()
	key := "key"
	expiration := 60 * time.Second // 60秒

	_, err := client.Set(ctx, key, "old value", expiration).Result()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if err := redisLock(ctx, client, key); err != nil {
		log.Printf("Error: %v", err)
	}

	ttl, err := client.TTL(ctx, key).Result()
	if err != nil {
		log.Printf("Error: %v", err)
	}
	log.Printf("TTL after pipeline: %v", ttl)
}
