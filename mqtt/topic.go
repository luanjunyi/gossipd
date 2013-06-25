package mqtt

import (
	"sync"
)

type Topic struct {
	Content string
	RetainedMessage *MqttMessage
}

var G_topics map[string]*Topic = make(map[string]*Topic)
var G_topics_lock *sync.Mutex = new(sync.Mutex)

func CreateTopic(content string) *Topic {
	topic := new(Topic)
	topic.Content = content
	topic.RetainedMessage = nil
	return topic
}