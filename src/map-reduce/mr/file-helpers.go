package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

func GenerateFileName(mapTask int, reduceTask int) string {
	filePath, err := filepath.Abs(strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask))

	if err != nil {
		fmt.Println("Could not create absolute file path")
	}

	return filePath
}

func GenerateTempFile() *os.File {
	tempFile, err := ioutil.TempFile("./", "*")
	if err != nil {
		fmt.Println("Failed to create temp file", err)
	}

	return tempFile
}

func ReadFile(filePath string) []byte {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()

	return content
}
