package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"http"
	"io"
	"os"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	sourceFilePath := os.Args[1]
	csvFile, _ := os.Open(sourceFilePath)
	reader := csv.NewReader(bufio.NewReader(csvFile))

	for {
		line, err := reader.Read()

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		client := http.Client{
			
		}

		fmt.Println(line)
	}

	//fmt.Println(string(data))
}