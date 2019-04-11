//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/dustin/go-humanize"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

var (
	slices = flag.Int("slices", 1, "number of components to split the download/upload into, anything between 1 and 31")
	buffer = flag.Int("buffer", 16, "size of disk copy buffer in KBs, linux does well with 16, Windows does better with higher sizes (like 2048)")

	gsRegex = regexp.MustCompile(`^gs://([a-z0-9][-_.a-z0-9]*)/(.+)$`)

	start time.Time
)

func splitGCSPath(p string) (string, string, error) {
	matches := gsRegex.FindStringSubmatch(p)
	if matches != nil {
		return matches[1], matches[2], nil
	}

	return "", "", fmt.Errorf("%q is not a valid GCS path", p)
}

func gcsToGCS(ctx context.Context, fromBkt, fromObj, toBkt, toObj string) {
	log.Println("Creating storage client")
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	log.Printf(`Beginning copy from "gs://%s/%s" to "gs://%s/%s"`, fromBkt, fromObj, toBkt, toObj)
	from := client.Bucket(fromBkt).Object(fromObj)
	if _, err := client.Bucket(fromBkt).Object(fromObj).CopierFrom(from).Run(ctx); err != nil {
		log.Fatal(err)
	}

	log.Print("Finished copying")
}

func localToLocal(from, to string) {
	src, err := os.Open(from)
	if err != nil {
		log.Fatal(err)
	}

	fi, err := src.Stat()
	if err != nil {
		log.Fatal(err)
	}
	totalSize := fi.Size()

	log.Println("Allocating local file")
	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Beginning copy from %q to %q", from, to)
	start = time.Now()
	if _, err := io.CopyBuffer(dst, src, make([]byte, *buffer*1024)); err != nil {
		log.Fatal(err)
	}

	if err := dst.Close(); err != nil {
		log.Fatal(err)
	}

	since := time.Since(start)
	spd := humanize.IBytes(uint64(totalSize / int64(since.Seconds())))
	total := humanize.IBytes(uint64(totalSize))
	log.Printf("Finished copying %s in %s [%s/s]", total, since, spd)
}

type offsetWriter struct {
	io.WriterAt
	offset int64
}

func (o *offsetWriter) Write(b []byte) (int, error) {
	n, err := o.WriteAt(b, o.offset)
	o.offset = o.offset + int64(n)
	return n, err
}

func gcsToLocal(ctx context.Context, fromBkt, fromObj, to string) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	obj := client.Bucket(fromBkt).Object(fromObj)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client.Close()
	gen := attrs.Generation
	totalSize := attrs.Size
	chunkSize := totalSize / int64(*slices)

	log.Println("Allocating local file")
	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(`Beginning copy from "gs://%s/%s" to %q`, fromBkt, fromObj, to)
	start = time.Now()
	var wg sync.WaitGroup
	for offset := int64(0); offset < totalSize; offset += chunkSize {
		wg.Add(1)
		go func(wg *sync.WaitGroup, offset, chunkSize int64) {
			defer wg.Done()
			// Making separate connections vastly increases speed.
			baseTransport := &http.Transport{
				DisableKeepAlives:     false,
				MaxIdleConns:          0,
				MaxIdleConnsPerHost:   1000,
				MaxConnsPerHost:       0,
				IdleConnTimeout:       60 * time.Second,
				ResponseHeaderTimeout: 5 * time.Second,
				TLSHandshakeTimeout:   5 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			}
			transport, err := htransport.NewTransport(ctx, baseTransport)
			if err != nil {
				log.Fatal(err)
			}
			client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: transport}))
			if err != nil {
				log.Fatal(err)
			}
			defer client.Close()

			obj := client.Bucket(fromBkt).Object(fromObj).Generation(gen)
			rr, err := obj.NewRangeReader(ctx, offset, chunkSize)
			if err != nil {
				log.Fatal(err)
			}
			defer rr.Close()

			if _, err := io.CopyBuffer(&offsetWriter{dst, offset}, rr, make([]byte, *buffer*1024)); err != nil {
				log.Fatal(err)
			}
		}(&wg, offset, chunkSize)
	}
	wg.Wait()

	if err := dst.Close(); err != nil {
		log.Fatal(err)
	}

	since := time.Since(start)
	spd := humanize.IBytes(uint64(totalSize / int64(since.Seconds())))
	total := humanize.IBytes(uint64(totalSize))
	log.Printf("Finished copying %s in %s [%s/s]", total, since, spd)
}

type offsetReader struct {
	io.ReaderAt
	offset int64
}

func (o *offsetReader) Read(b []byte) (int, error) {
	n, err := o.ReadAt(b, o.offset)
	o.offset = o.offset + int64(n)
	return n, err
}

func localToGCS(ctx context.Context, from, toBkt, toObj string) {
	log.Println("Creating storage client")
	baseTransport := &http.Transport{
		DisableKeepAlives:     false,
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       60 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	transport, err := htransport.NewTransport(ctx, baseTransport)
	if err != nil {
		log.Fatal(err)
	}
	client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	src, err := os.Open(from)
	if err != nil {
		log.Fatal(err)
	}

	fi, err := src.Stat()
	if err != nil {
		log.Fatal(err)
	}
	totalSize := fi.Size()

	log.Printf(`Beginning copy from %q to "gs://%s/%s"`, from, toBkt, toObj)
	start = time.Now()
	if *slices == 1 {
		dst := client.Bucket(toBkt).Object(toObj).NewWriter(ctx)
		if _, err := io.Copy(dst, src); err != nil {
			log.Fatal(err)
		}

		if err := dst.Close(); err != nil {
			log.Fatal(err)
		}
	} else {
		chunkSize := totalSize / int64(*slices)
		var wg sync.WaitGroup
		var tmpObjs []*storage.ObjectHandle
		for offset := int64(0); offset < totalSize; offset += chunkSize {
			wg.Add(1)
			tmpObj := fmt.Sprint(toObj, "/go-gcs-cp.tmp", offset)
			tmpObjs = append(tmpObjs, client.Bucket(toBkt).Object(tmpObj))
			go func(tmpObj string, wg *sync.WaitGroup, offset, chunkSize int64) {
				defer wg.Done()
				// Making separate connections vastly increases speed.
				baseTransport := &http.Transport{
					DisableKeepAlives:     false,
					MaxIdleConns:          0,
					MaxIdleConnsPerHost:   1000,
					MaxConnsPerHost:       0,
					IdleConnTimeout:       60 * time.Second,
					ResponseHeaderTimeout: 5 * time.Second,
					TLSHandshakeTimeout:   5 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				}
				transport, err := htransport.NewTransport(ctx, baseTransport)
				if err != nil {
					log.Fatal(err)
				}
				client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: transport}))
				if err != nil {
					log.Fatal(err)
				}
				defer client.Close()

				dst := client.Bucket(toBkt).Object(tmpObj).NewWriter(ctx)
				if _, err := io.CopyN(dst, &offsetReader{src, offset}, chunkSize); err != nil {
					if io.EOF != err {
						log.Fatal(err)
					}
				}

				if err := dst.Close(); err != nil {
					log.Fatal(err)
				}
			}(tmpObj, &wg, offset, chunkSize)
		}
		wg.Wait()

		// Compose the object.
		if _, err := client.Bucket(toBkt).Object(toObj).ComposerFrom(tmpObjs...).Run(ctx); err != nil {
			log.Fatal(err)
		}
		// Cleanup.
		for _, o := range tmpObjs {
			o.Delete(ctx)
		}
	}

	since := time.Since(start)
	spd := humanize.IBytes(uint64(totalSize / int64(since.Seconds())))
	total := humanize.IBytes(uint64(totalSize))
	log.Printf("Finished copying %s in %s [%s/s]", total, since, spd)
}

func main() {
	flag.Parse()
	ctx := context.Background()

	if len(flag.Args()) < 2 {
		log.Fatalf("not enough args, usage: %s [FROM] [TO]", os.Args[0])
	}
	from, to := flag.Arg(0), flag.Arg(1)

	if 0 > *slices || *slices > 31 {
		log.Fatal("-slices flag should be between 1 and 31")
	}

	var fromGCS, toGCS bool

	fromBkt, fromObj, err := splitGCSPath(from)
	if err == nil {
		fromGCS = true
	}

	toBkt, toObj, err := splitGCSPath(to)
	if err == nil {
		toGCS = true
	}

	switch {
	case fromGCS && toGCS:
		gcsToGCS(ctx, fromBkt, fromObj, toBkt, toObj)
	case !fromGCS && !toGCS:
		localToLocal(from, to)
	case fromGCS && !toGCS:
		gcsToLocal(ctx, fromBkt, fromObj, to)
	case !fromGCS && toGCS:
		localToGCS(ctx, from, toBkt, toObj)
	}
}
