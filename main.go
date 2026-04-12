package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ankush/go-jobs/db"
	"github.com/ankush/go-jobs/handler"
	"github.com/ankush/go-jobs/models"
	"github.com/ankush/go-jobs/worker"
	"github.com/gin-gonic/gin"
)

func main() {
	db.Connect()
	db.ConnectRedis()

	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_free", "workers", "0")
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_premium", "workers", "0")

	err := db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream", "workers", "0").Err()
	if err != nil {
		log.Println("Consumer group may already exists", err)
	}

	db.DB.AutoMigrate(&models.Job{})

	r := gin.Default()
	//routes
	r.POST("/jobs", handler.CreateJob)
	r.GET("/jobs", handler.GetJobs)
	r.GET("/jobs/:id", handler.GetJobsById)

	//start worker here
	go worker.StartWorker("worker-free", "jobs_stream_free")
	go worker.StartWorker("worker-premium", "jobs_stream_premium")

	//listen for the ctrl + c
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGABRT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Printf("Shutting down workers...")

		worker.Shutdown = true

		worker.Wg.Wait()

		log.Printf("All jobs completed..EXITING")
		os.Exit(0)
	}()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Server running"})
	})
	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	//run server in goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit = make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	//BLOCK here (this is key)
	<-quit

	log.Println("Shutting down workers...")

	worker.Shutdown = true

	//wait for all worker goroutines
	worker.Wg.Wait()

	log.Println("Shutting down HTTP server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("Server forced to shutdown:", err)
	}

	log.Println("All jobs completed. Exiting.")
}
