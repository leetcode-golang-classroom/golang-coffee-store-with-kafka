package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/application"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/config"
)

func main() {
	app := application.New(config.AppConfig)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	err := app.Start(ctx)
	if err != nil {
		log.Println("failed to start coffee shop server:", err)
	}
}
