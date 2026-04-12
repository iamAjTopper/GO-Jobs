package models

import "time"

type Job struct {
	ID        uint `gorm:"primarykey"`
	Type      string
	Paylaod   string
	Status    string
	Retries   int
	Processed bool   `gorm:"default:false"`
	Priority  string `gorm:"default:free"`
	CreatedAt time.Time
	UpdatedAt time.Time
}
