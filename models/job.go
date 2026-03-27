package models

import "time"

type Job struct {
	ID        uint `gorm:"primarykey"`
	Type      string
	Paylaod   string
	Status    string
	Retries   int
	CreatedAt time.Time
	UpdatedAt time.Time
}
