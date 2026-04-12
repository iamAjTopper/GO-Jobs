package models

type Outbox struct {
	ID     uint `gorm:"primary_key"`
	JobID  uint
	Status string //pending or senmt
	Stream string //which reddis stream to publish
}
