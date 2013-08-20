#What is Gossipd?#
Gossipd is an implementation of [MQTT 3.1](http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html) broker written in Go. MQTT is an excellent protocal for mobile messaging. Facebook built the new Facebook Messager based on MQTT.

#What is Gossipd for?#
The main reason I'm writting this is because all major open source MQTT brokers, like [Mosquitto](http://mosquitto.org/) didn't balance well between scalability and ease of maintaining.

Gossipd should be considered when the 'select' based Mosquitto can't meet your scale requirement, yet only the 'basic' part(see [Not supported features](#unsupported) for detail) of MQTT is needed in your project. Gossipd is built with profermance at heart.

#Usage#
Super simple. Just run 

>go run gossipd 

The broker will start and listen on port 1883 for MQTT traffic. Command line flags:

* -p PORT: specify MQTT broker's port, default is 1883
* -r PORT: specify Redis's port, default is 6379
* -d: when set comprehensive debugging info will be printed, this may significantly harm performance.

#Dependency#
* [Redis](http://redis.io) for storage
* [Redigo](https://github.com/garyburd/redigo) as Redis binding for golang.
* [Seelog](https://github.com/cihub/seelog) for logging

#<a id="unsupported"></a>Not supported features#
* QOS level 2 is not supported. Only support QOS 1 and 0.

* Topic wildcard is not supported. Topic is always treated as plain string.
