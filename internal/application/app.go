package application

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/broker"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/config"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/types"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/util"
)

type App struct {
	config      *config.Config
	mux         *http.ServeMux
	broker      *broker.Broker
	orderWorker types.Worker
}

func New(config *config.Config) *App {
	broker, err := broker.NewBroker(config.BrokerURL)
	if err != nil {
		util.FailOnError(err, "failed on connect to kafka")
	}
	app := &App{
		mux:    http.NewServeMux(),
		config: config,
		broker: broker,
	}
	// set up routes
	app.SetUpRoutes()
	// set up worker
	app.SetupWorker()
	return app
}

func (app *App) Start(ctx context.Context) error {
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", app.config.Port),
		Handler: app.mux,
	}
	log.Printf("Starting server on %d\n", app.config.Port)
	errCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			errCh <- fmt.Errorf("failed to start server: %w", err)
		}
		util.CloseChannel(errCh)
	}()
	defer func() {
		if err := app.broker.ClosePublisher(); err != nil {
			log.Printf("failed to close Publisher %v\n", err)
		}
		if err := app.broker.CloseConsumer(); err != nil {
			log.Printf("failed to close Publisher %v\n", err)
		}
	}()
	go func() {
		err := app.orderWorker.Run(ctx)
		if err != nil {
			errCh <- fmt.Errorf("failed to run worker: %w", err)
		}
		util.CloseChannel(errCh)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		timeout, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		return server.Shutdown(timeout)
	}
}
