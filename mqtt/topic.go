package mqtt

import (
	"sync"
)

type Topic struct {
	Content string
	RetainedContent string
}

var G_topics map[string]*Topic = make(map[string]*Topic)
var G_topics_lock *sync.Mutex = new(sync.Mutex)

func CreateTopic(content string) *Topic {
	topic := new(Topic)
	topic.Content = content
	return topic
}