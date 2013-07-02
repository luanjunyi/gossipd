package mqtt

import (
	"sync"
)

type Topic struct {
	Content string
	RetainedMessage *MqttMessage
}

var G_topicss map[string]*Topic = make(map[string]*Topic)
var G_topics_lockk *sync.Mutex = new(sync.Mutex)

func CreateTopic(content string) *Topic {
	topic := new(Topic)
	topic.Content = content
	topic.RetainedMessage = nil
	return topic
}