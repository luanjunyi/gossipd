package main

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
)

func main() {
	fmt.Println("Testing Redis...")
	c, err := redis.Dial("tcp", "localhost:6379")
	
	if err != nil {
		fmt.Println("failed to connec")
	} else {
		fmt.Println("connected")
	}

	s, _ := redis.String(c.Do("Get", "target"))
	fmt.Println(s)
	c.Close()
}