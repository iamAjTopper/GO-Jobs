package workerpkg

import (
	"log"
	"time"

	"github.com/ankush/go-jobs/shared/db"
	"github.com/ankush/go-jobs/shared/models"
	"github.com/redis/go-redis/v9"
)

// this worker reads ther outbox table and ;uashres jobs to redis
func StartOutBoxProcessor() {
	for {
		var events []models.Outbox
		//garbiong upto 10 pending events at one time to mot pverload m,em,eory
		db.DB.Where("status = ?", "pending").Limit(10).Find(&events)

		//loop through every event
		for _, event := range events {
			//trying to move the ecvent from the darabse to redios
			_, err := db.RDB.XAdd(db.Ctx, &redis.XAddArgs{
				Stream: event.Stream,
				Values: map[string]interface{}{
					"job_id": event.JobID,
				},
			}).Result()

			if err != nil {
				log.Println("Failed to push to Reddis:", err)
				continue
			}
			//mark as sent
			db.DB.Model(&event).Update("status", "sent")
		}
		time.Sleep(2 * time.Second)
	}
}
