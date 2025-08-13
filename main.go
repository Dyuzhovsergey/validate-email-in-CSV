package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/mail"
	"os"
	"strings"
	"sync"
	"time"
)

type Job struct {
	record []string
}

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
	if email == "" {
		return false
	}
	_, err := mail.ParseAddress(email)
	return err == nil
}

func worker(jobs <-chan Job, results chan<- []string, emailIdx int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		email := ""
		if emailIdx >= 0 && emailIdx < len(job.record) {
			email = job.record[emailIdx]
		}
		valid := validateEmail(email)
		result := append(job.record, fmt.Sprintf("%t", valid))
		results <- result
	}
}

func main() {
	// ======== ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ========
	startParallel := time.Now()

	reader, inFile := openCSV("input.csv")
	defer inFile.Close()

	writer, outFile := createCSV("output_parallel.csv")
	defer outFile.Close()
	defer writer.Flush()

	header, err := reader.Read()
	if err != nil {
		log.Fatalf("Error read header: %v", err)
	}

	// find email index
	emailIdx := -1
	for i, h := range header {
		if strings.ToLower(strings.TrimSpace(h)) == "email" {
			emailIdx = i
			break
		}
	}

	newHeader := append(header, "email_valid")
	writer.Write(newHeader)

	jobs := make(chan Job, 100)
	results := make(chan []string, 100)

	var wg sync.WaitGroup
	numWorkers := 4

	// запускаем воркеров
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, emailIdx, &wg)
	}

	// писатель результатов в отдельной горутине
	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		for res := range results {
			writer.Write(res)
		}
	}()

	// читаем входной CSV и отправляем в jobs
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		jobs <- Job{record: record}
	}

	close(jobs) // сигнал воркерам
	wg.Wait()   // ждём всех воркеров
	close(results)
	writeWG.Wait()

	fmt.Println("parallel processing took:", time.Since(startParallel))

	// ======== ПОСЛЕДОВАТЕЛЬНАЯ ОБРАБОТКА ========
	startSequential := time.Now()

	readerSeq, inFileSeq := openCSV("input.csv")
	defer inFileSeq.Close()

	writerSeq, outFileSeq := createCSV("output_sequential.csv")
	defer outFileSeq.Close()
	defer writerSeq.Flush()

	headerSeq, err := readerSeq.Read()
	if err != nil {
		log.Fatalf("Error read header: %v", err)
	}

	emailIdxSeq := -1
	for i, h := range headerSeq {
		if strings.ToLower(strings.TrimSpace(h)) == "email" {
			emailIdxSeq = i
			break
		}
	}

	newHeaderSeq := append(headerSeq, "email_valid")
	writerSeq.Write(newHeaderSeq)

	for {
		record, err := readerSeq.Read()
		if err != nil {
			break
		}
		email := ""
		if emailIdxSeq >= 0 && emailIdxSeq < len(record) {
			email = record[emailIdxSeq]
		}
		valid := validateEmail(email)
		result := append(record, fmt.Sprintf("%t", valid))
		writerSeq.Write(result)
	}

	fmt.Println("sequential processing took:", time.Since(startSequential))
}
