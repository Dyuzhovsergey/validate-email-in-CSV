package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/mail"
	"os"
	"strings"
	"sync"
)

type Job struct {
	record []string
}

var mu sync.Mutex

func openCSV(path string) (*csv.Reader, *os.File) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error open file: %v", err)
	}
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	return reader, file
}

func createCSV(path string) (*csv.Writer, *os.File) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Error create file: %v", err)
	}
	writer := csv.NewWriter(file)
	return writer, file
}

func validateEmail(email string) bool {
	email = strings.TrimSpace(email)
	_, err := mail.ParseAddress(email)
	return err == nil
}

// worker
func worker(jobs <-chan Job, writer *csv.Writer, emailIdx int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		email := ""
		if emailIdx >= 0 && emailIdx < len(job.record) {
			email = job.record[emailIdx]
		}
		valid := validateEmail(email)

		result := append(job.record, fmt.Sprintf("%t", valid))

		mu.Lock()
		writer.Write(result)
		mu.Unlock()
	}
}

func main() {
	// Read input CSV
	reader, inFile := openCSV("input.csv")
	defer inFile.Close()

	// Create uotput CSV
	writer, outFile := createCSV("output.csv")
	defer outFile.Close()
	defer writer.Flush()

	// Read header
	header, err := reader.Read()
	if err != nil {
		log.Fatalf("Error read header: %v", err)
	}

	// Email index
	emailIdx := -1
	for i, h := range header {
		if strings.ToLower(strings.TrimSpace(h)) == "email" {
			emailIdx = i
			break
		}
	}

	newHeader := append(header, "email_valid")
	writer.Write(newHeader)

	// Run workers
	jobs := make(chan Job, 100)
	var wg sync.WaitGroup
	numWorkers := 4

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, writer, emailIdx, &wg)
	}

	// Read string and receive to chanel jobs
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		jobs <- Job{record: record}
	}

	close(jobs)
	wg.Wait()

	log.Println("Обработка завершена!")
}
