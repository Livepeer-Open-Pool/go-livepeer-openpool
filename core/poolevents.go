package core

import (
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
)

type PoolEventTracker struct {
	db *common.DB
}

func NewPoolEventTracker(db *common.DB) PoolEventTracker {
	glog.Info("PoolEventTracker NewPoolEventTracker creating new instance of PoolEventTracker")

	return PoolEventTracker{
		db: db,
	}
}

func (p *PoolEventTracker) CreateEventLog(eventType string, pairs ...interface{}) {
	glog.Info("PoolEventTracker CreateEventLog eventType=", eventType, " pairs=", pairs, "")

	err := p.db.CreateEventLog(eventType, pairs...)
	if err != nil {
		glog.Info("PoolEventTracker CreateEventLog error goo boom=", err)
	}
}
