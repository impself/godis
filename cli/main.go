package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

func main() {
	port := "6399"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	rdb := redis.NewClient(&redis.Options{Addr: ":" + port})
	ctx := context.Background()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("go-redis-cli -p %s> ", port)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Printf("> ")
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}

		parts := strings.Fields(line)
		args := make([]interface{}, len(parts))
		for i, p := range parts {
			args[i] = p
		}

		result, err := rdb.Do(ctx, args...).Result()
		if err != nil {
			fmt.Println("ERR:", err)
		} else {
			fmt.Println(result)
		}
		fmt.Printf("> ")
	}
}
