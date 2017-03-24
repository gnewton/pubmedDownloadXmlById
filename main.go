package main

/*
 Get Mesh terms for PMIDs
 1 - Read pmids from stdin
     i.e. "bunzip2 -c pmid_distinct_random_sorted.txt.bz2 |..."
 2 - Create 1 + n files:
   pubmed.mesh.gz - id on one line followed by indented mesh terms, one per line
    pubmed_xml_0.gz
    pubmed_xml_....n.gz
    XML downloaded, in 5000 record chunks

Find largest PMID: MAX PMID: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&reldate=60&datetype=pdat&retmax=1

NCBI: large jobs:
"In order not to overload the E-utility servers, NCBI recommends that users post no more than three URL requests per second and limit large jobs to either weekends or between 9:00 PM and 5:00 AM Eastern time during weekdays. "
  - https://www.ncbi.nlm.nih.gov/books/NBK25497/

*/

import (
	"bufio"
	//"bytes"
	"compress/gzip"
	"crypto/tls"
	//"encoding/xml"
	//"fmt"
	//	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"github.com/gnewton/gopubmed"
	//	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var recordsPerFile = 50000

//////////
var recordsPerHttpRequest = 50
var recordsPerHttpRequestAfterHours = 150

var timeMin float64 = 2.0
var timeMax float64 = 8.0

var timeMinAfterHours float64 = 0.25
var timeMaxAfterHours float64 = 1.5

////////

var readFromStdin = false
var writeMesh = false
var defaultIdFile = "ids.txt"

const baseXmlFileName = "pubmed_xml_"

var meshFile = "pubmed.mesh.gz"
var inputFileName = ""

func init() {
	//gopubmed.Debug = true
	flag.StringVar(&inputFileName, "f", inputFileName, "Name of input file with one pmid per line, if used")
	flag.StringVar(&meshFile, "M", meshFile, "File to write pmids and mesh terms")

	flag.BoolVar(&readFromStdin, "c", readFromStdin, "Read pmids from stdin, one per line")
	flag.IntVar(&recordsPerFile, "n", recordsPerFile, "Number of records per output file")
	flag.IntVar(&recordsPerHttpRequest, "t", recordsPerHttpRequest, "Number of records per http request to pubmed")
	flag.IntVar(&recordsPerHttpRequestAfterHours, "T", recordsPerHttpRequestAfterHours, "Number of records per http request to pubmed, after hours")

	flag.Parse()
	if inputFileName == "" || !readFromStdin {
		fmt.Println("Either set -c for stdin or -f to read from a file\n")
		flag.Usage()
		os.Exit(1)
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tr := &http.Transport{
		ResponseHeaderTimeout: time.Second * 500,
		DisableKeepAlives:     false,
		DisableCompression:    false,
	}

	var wXml *gzip.Writer = nil
	var ww *bufio.Writer = nil
	var xFile *os.File = nil

	meshFile, err2 := os.Create(meshFile)
	if err2 != nil {
		return
	}
	defer meshFile.Close()
	wwMesh := bufio.NewWriter(meshFile)
	wMesh := gzip.NewWriter(wwMesh)

	//w := bufio.NewWriter(file)

	numIdsPerUrl := findNumIdsPerUrl()
	pmids := make([]string, numIdsPerUrl)

	urlFetcher := gopubmed.Fetcher{
		Ssl: true,
		Transport: &http.Transport{
			ResponseHeaderTimeout: time.Second * 500,
			DisableKeepAlives:     false,
			DisableCompression:    false,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		},
	}

	allCount := 0
	count := 0

	reader, err := makeReader()
	if err != nil {
		log.Fatal(err)
	}
	first := true
	chunkCount := 0
	for {
		thisNumIdsPerUrl := findNumIdsPerUrl()
		if numIdsPerUrl != thisNumIdsPerUrl {
			numIdsPerUrl = thisNumIdsPerUrl
			pmids = make([]string, numIdsPerUrl)
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			// You may check here if err == io.EOF
			break
		}
		line = strings.TrimSpace(line)
		err = lineChecker(line)
		if err != nil {
			log.Fatal(err)
		}
		//log.Println(line)
		pmids[count] = line

		if wXml == nil {
			wXml, ww, xFile = makeXmlWriter(allCount, pmids[0])
		}

		count = count + 1
		// Collected enough pmids: get their XML from NIH
		if count == numIdsPerUrl {
			getPubmedRecords(&urlFetcher, first, wMesh, wXml, tr, pmids)
			checkTime()
			first = false
			count = 0
			zeroArray(pmids)
		} else {

		}
		allCount += 1
		chunkCount += 1
		// Start new xml file: close old one: open new one
		if chunkCount > recordsPerFile {
			fmt.Fprintln(wXml, endPubmedArticleSet)
			wXml.Flush()
			wXml.Close()
			ww.Flush()
			wXml, ww, xFile = makeXmlWriter(allCount, pmids[0])
			chunkCount = 0
			first = true
		}
		if allCount%500 == 0 {
			log.Println(allCount)
		}
	}
	if count != 0 {
		getPubmedRecords(&urlFetcher, first, wMesh, wXml, tr, pmids)
	}
	fmt.Fprintln(wXml, endPubmedArticleSet)

	wXml.Flush()
	wXml.Close()
	ww.Flush()

	wMesh.Flush()
	wwMesh.Flush()
	wMesh.Close()

	xFile.Close()
}

func zeroArray(a []string) {
	for i := 0; i < len(a); i++ {
		a[i] = ""
	}
}

func getPubmedRecords(urlFetcher *gopubmed.Fetcher, first bool, meshWriter *gzip.Writer, xmlWriter *gzip.Writer, transport *http.Transport, pmids []string) {
	preUrlTime := time.Now()

	articles, raw, err := urlFetcher.GetArticlesAndRaw(pmids)
	if err != nil {
		log.Fatal(err)
	}
	s := string(raw[:len(raw)])

	for i := 0; i < len(articles); i++ {
		pubmedArticle := articles[i]
		if pubmedArticle.MedlineCitation != nil && pubmedArticle.MedlineCitation.MeshHeadingList != nil && pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading != nil {
			fmt.Fprint(meshWriter, articles[i].MedlineCitation.PMID.Text)
			for j := 0; j < len(pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading); j++ {
				fmt.Fprint(meshWriter, "|")
				fmt.Fprint(meshWriter, pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].DescriptorName.Attr_UI)
				fmt.Fprint(meshWriter, "::"+pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].DescriptorName.Text)

				if len(pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].QualifierName) > 0 {
					fmt.Fprint(meshWriter, "=")
					for q := 0; q < len(pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].QualifierName); q++ {
						if q != 0 {
							fmt.Fprint(meshWriter, "&")
						}

						fmt.Fprint(meshWriter, pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].QualifierName[q].Attr_UI)
						fmt.Fprint(meshWriter, "::"+pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].QualifierName[q].Text)
					}
				}
			}
			fmt.Fprintln(meshWriter, "")
		}
	}
	meshWriter.Flush()
	if !first {
		s = removeFirstNLines(s, 3)
		// s = strings.Replace(s, startXml, "", -1)
		// s = strings.Replace(s, docType, "", -1)
		// s = strings.Replace(s, startPubmedArticleSet, "", -1)
	}
	s = strings.Replace(s, endPubmedArticleSet, "<!-- breakset -->", -1)

	xmlWriter.Write([]byte(s))
	postUrlTime := time.Now()
	log.Println("Total request time:", postUrlTime.Sub(preUrlTime))
}

func makeXmlWriter(fileCount int, startPmid string) (*gzip.Writer, *bufio.Writer, *os.File) {
	//xmlFile, err := os.Create("./" + baseXmlFileName + strconv.Itoa(fileCount) + "_" + strconv.Itoa(fileCount+recordsPerFile) + ".gz")
	xmlFile, err := os.Create("./" + baseXmlFileName + startPmid + ".gz")
	if err != nil {
		return nil, nil, nil
	}
	ww := bufio.NewWriter(xmlFile)
	return gzip.NewWriter(ww), ww, xmlFile
}

func findNumIdsPerUrl() int {
	if afterHours() {
		return recordsPerHttpRequestAfterHours
	}
	return recordsPerHttpRequest
}

func makeDelayTime() float64 {
	if afterHours() {
		log.Println("AFTER HOURS ...")
		return timeMinAfterHours + rand.Float64()*timeMaxAfterHours
	} else {
		return timeMin + rand.Float64()*timeMax
	}
}

func afterHours() bool {
	now := time.Now().UTC()
	hour, _, _ := now.Clock()
	//return hour < 8 || hour > 18
	// hours in UTC; need US East coast (location of NCBI)
	return hour < 12 || hour > 22
}

func checkTime() {
	now := time.Now()
	rand.Seed(int64(now.Nanosecond()/9999999 + now.Second()*100 + now.Hour()))
	sleepSeconds := makeDelayTime()

	duration := (time.Duration)(sleepSeconds)
	t0 := time.Now()

	time.Sleep(duration * time.Second)
	t1 := time.Now()
	log.Println("End sleep:", t1.Sub(t0))
}

func makeReader() (*bufio.Reader, error) {
	var inputFile *os.File
	if readFromStdin {
		log.Println("*************************************************")
		inputFile = os.Stdin
	} else {
		if inputFileName == "" {
			return nil, errors.New("Empty input file name")
		}
		var err error
		inputFile, err = os.Open(inputFileName)
		if err != nil {
			return nil, err
		}
	}
	reader := bufio.NewReader(inputFile)
	return reader, nil
}

func lineChecker(l string) error {
	if len(l) == 0 {
		return errors.New("Error: Empty line")
	}
	var n int
	var err error
	if n, err = strconv.Atoi(l); err != nil {
		return errors.New("Error: Expecting pmid (integer); found[" + l + "]")
	}
	if n <= 0 {
		return errors.New("Error: pmids are positive integers; found [" + l + "]")
	}
	return nil
}

func removeFirstNLines(s string, n int) string {
	for i := 0; i < 3; i++ {
		n := strings.Index(s, "\n")
		s = s[n+1:]
	}
	return s
}
