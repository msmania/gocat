package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	MaxRetry      int
	BatchSizeInMB int
)

func checkHeaders(url string) (int64, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, err
	}

	acceptRanges := resp.Header.Get("Accept-Ranges")
	if acceptRanges != "bytes" {
		return 0, errors.New("no supported Accept-Ranges found")
	}

	contentLenStr := resp.Header.Get("Content-Length")
	contentLen, err := strconv.ParseInt(contentLenStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return contentLen, nil
}

func downloadChunk(
	client http.Client,
	url string,
	offsetFrom, offsetTo int64,
) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	rangeStr := fmt.Sprintf("bytes=%v-%v", offsetFrom, offsetTo)
	req.Header.Add("Range", rangeStr)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBytes, nil
}

func downloadChunkWithRetry(
	client http.Client,
	url string,
	offsetFrom, offsetTo int64,
) (resp []byte, err error) {
	for i := 0; i < MaxRetry; i++ {
		resp, err = downloadChunk(client, url, offsetFrom, offsetTo)
		if err == nil {
			break
		}
		fmt.Fprintf(
			os.Stderr,
			"[%v] retrying %v/%v (%v)\n",
			time.Now().Format(time.RFC3339),
			i,
			MaxRetry,
			err.Error(),
		)
		time.Sleep(time.Second)
	}
	return
}

func downloadAndWrite(url string, w io.Writer) error {
	contentLen, err := checkHeaders(url)
	if err != nil {
		return err
	}

	batchSize := int64(BatchSizeInMB) << 20
	numChunks := contentLen / batchSize
	if contentLen%batchSize > 0 {
		numChunks++
	}

	client := http.Client{}

	chunk := int64(1)
	for offset := int64(0); offset < contentLen; {
		offsetTo := offset + batchSize
		if offsetTo > contentLen {
			offsetTo = contentLen
		}

		fmt.Fprintf(
			os.Stderr,
			"[%v] downloading %v/%v [%v, %v) from %s\n",
			time.Now().Format(time.RFC3339),
			chunk,
			numChunks,
			offset,
			offsetTo,
			url,
		)
		resp, err := downloadChunkWithRetry(client, url, offset, offsetTo-1)
		if err != nil {
			return err
		}

		if _, err = w.Write(resp); err != nil {
			return err
		}

		offset = offsetTo
		chunk++
	}

	return nil
}

func downloadList(url string) ([]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	list := []string{}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "http") {
			continue
		}

		list = append(list, line)

		err := scanner.Err()
		if err != nil {
			return nil, err
		}
	}

	return list, nil
}

func printUsage() {
	fmt.Fprintln(
		os.Stderr,
		"Usage: gocat -m <max retry> -b <batch size in MB> <url>",
	)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	flag.IntVar(&MaxRetry, "m", 100, "max download retry attempts")
	flag.IntVar(&BatchSizeInMB, "b", 16, "chunk size")
	flag.Parse()

	url := flag.Arg(flag.NArg() - 1)
	files, err := downloadList(url)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if err := downloadAndWrite(file, os.Stdout); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Fprintln(os.Stderr, "COMPLETED!")
}
