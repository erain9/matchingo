package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/erain9/matchingo/pkg/api/proto"
	"github.com/erain9/matchingo/pkg/db/queue"
	"github.com/erain9/matchingo/pkg/messaging"
	"github.com/erain9/matchingo/pkg/server"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	grpcPort  = flag.Int("grpc_port", 50051, "The gRPC server port")
	httpPort  = flag.Int("http_port", 8080, "The HTTP server port")
	logLevel  = flag.String("log_level", "info", "Log level: debug, info, warn, error")
	logFormat = flag.String("log_format", "pretty", "Log format: json, pretty")
)

func main() {
	// Parse command line flags
	flag.Parse()

	// Setup logging
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("Invalid log level: %v", err)
	}

	// Configure global logger
	logger := zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
	if *logFormat == "pretty" {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	}

	// Create default context with logger
	ctx := logger.WithContext(context.Background())

	// Create a new order book manager
	manager := server.NewOrderBookManager()
	defer manager.Close()

	// Create a test order book
	_, err = manager.CreateMemoryOrderBook(ctx, "test")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create test order book")
	}

	logger.Info().Str("name", "test").Msg("Created test order book")

	// Initialize Kafka consumer (optional)
	// The consumer is for developer purpose which helps pretty print the message
	// in the queue.
	var kafkaConsumer *queue.QueueMessageConsumer
	kafkaConsumer, err = queue.NewQueueMessageConsumer()
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to create Kafka consumer - continuing without Kafka support")
	} else {
		defer kafkaConsumer.Close()

		// Start Kafka consumer in a goroutine
		go func() {
			logger.Info().Msg("Starting Kafka consumer")
			err := kafkaConsumer.ConsumeDoneMessages(func(msg *messaging.DoneMessage) error {
				logger.Info().
					Str("order_id", msg.OrderID).
					Str("executed_qty", msg.ExecutedQty).
					Str("remaining_qty", msg.RemainingQty).
					Strs("canceled", msg.Canceled).
					Strs("activated", msg.Activated).
					Bool("stored", msg.Stored).
					Str("quantity", msg.Quantity).
					Str("processed", msg.Processed).
					Str("left", msg.Left).
					Interface("trades", msg.Trades).
					Msg("Received done message")
				return nil
			})
			if err != nil {
				logger.Error().Err(err).Msg("Kafka consumer error")
			}
		}()
	}

	// Start gRPC server
	grpcAddr := fmt.Sprintf(":%d", *grpcPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal().Err(err).Str("addr", grpcAddr).Msg("Failed to listen")
	}

	// Create gRPC server with the order book service
	grpcServer := grpc.NewServer()
	orderBookService := server.NewGRPCOrderBookService(manager)
	proto.RegisterOrderBookServiceServer(grpcServer, orderBookService)

	// Enable reflection for tools like grpcurl
	reflection.Register(grpcServer)

	// Start gRPC server in a goroutine
	go func() {
		logger.Info().Str("addr", grpcAddr).Msg("Starting gRPC server")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal().Err(err).Msg("Failed to serve gRPC")
		}
	}()

	// Start HTTP server for REST API (optional)
	httpAddr := fmt.Sprintf(":%d", *httpPort)
	httpServer := &http.Server{
		Addr: httpAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Add request context with logger
			reqLogger := logger.With().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("remote_addr", r.RemoteAddr).
				Logger()
			ctx := reqLogger.WithContext(r.Context())

			// Simple welcome page
			if r.URL.Path == "/" {
				w.Header().Set("Content-Type", "text/html")
				fmt.Fprintf(w, "<html><body>")
				fmt.Fprintf(w, "<h1>Matchingo Order Book Server</h1>")
				fmt.Fprintf(w, "<p>The gRPC server is running on port %d</p>", *grpcPort)
				fmt.Fprintf(w, "<p>This is a simple HTTP interface. Use the gRPC client for full functionality.</p>")
				fmt.Fprintf(w, "</body></html>")
				return
			}

			http.NotFound(w, r.WithContext(ctx))
		}),
	}

	// Start HTTP server in a goroutine
	go func() {
		logger.Info().Str("addr", httpAddr).Msg("Starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("Failed to serve HTTP")
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info().Str("signal", sig.String()).Msg("Received signal, shutting down")

	// Graceful shutdown
	grpcServer.GracefulStop()

	// Create a context with timeout for HTTP server shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server shutdown error")
	}

	logger.Info().Msg("Servers shutdown complete")
}
