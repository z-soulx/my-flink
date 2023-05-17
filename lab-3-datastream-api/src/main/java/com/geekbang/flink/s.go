package main

import "fmt"

func main() {
	for _, c := range "Hello, 世界" {
		fmt.Printf("%c", c)
	}

	for c := range "Hello, 世界" {
		fmt.Println(c)
	}
}
