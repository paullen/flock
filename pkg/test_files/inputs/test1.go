package main

import (
	"fmt"
)

func SayHi(g int) {
	fmt.Println("Hi", g)
}

func SayHello() {
	fmt.Println("Hello")
}

var FM = map[string]interface{}{"SayHi": SayHi, "SayHello": SayHello}
