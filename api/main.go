package main

import (
	"log"

	"github.com/ankush/go-jobs/api/handler"
	"github.com/ankush/go-jobs/shared/db"
	"github.com/ankush/go-jobs/shared/models"
	"github.com/gin-gonic/gin"
<<<<<<< HEAD
=======
	"github.com/ankush/go-jobs/shared/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
>>>>>>> master
)

func main() {

	db.Connect()
	db.ConnectRedis()

<<<<<<< HEAD
=======
	metrics.Init()

>>>>>>> master
	//create streams (safe to keep here OR move later)
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_free", "workers", "0")
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_premium", "workers", "0")

	db.DB.AutoMigrate(&models.Job{}, &models.Outbox{})

	r := gin.Default()

	//routes
	r.POST("/jobs", handler.CreateJob)
	r.GET("/jobs", handler.GetJobs)
	r.GET("/jobs/:id", handler.GetJobsById)
<<<<<<< HEAD
=======
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
>>>>>>> master

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Server running"})
	})

	log.Println("API running on :8080")
	r.Run(":8080")
}
