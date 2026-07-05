package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ankush/go-jobs/shared/db"
	"github.com/ankush/go-jobs/shared/models"
	"github.com/ankush/go-jobs/workerpkg"
<<<<<<< HEAD
=======
	"github.com/ankush/go-jobs/shared/metrics"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	
>>>>>>> master
)

func main() {

	db.Connect()
	db.ConnectRedis()

<<<<<<< HEAD
=======
	metrics.Init()
	go func() {
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Worker metrics running on :2112")
	http.ListenAndServe(":2112", nil)
	}()

>>>>>>> master
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_free", "workers", "0")
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_premium", "workers", "0")

	db.DB.AutoMigrate(&models.Job{}, &models.Outbox{})

	go workerpkg.StartWorker("worker-free", "jobs_stream_free")
	go workerpkg.StartWorker("worker-premium", "jobs_stream_premium")

	log.Println("Worker running...")

	go workerpkg.StartOutBoxProcessor()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit

	log.Println("Shutting down workers...")

	workerpkg.Shutdown = true
	workerpkg.Wg.Wait()

	log.Println("All jobs completed.")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = ctx
}
