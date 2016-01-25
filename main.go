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

NCBI: large jobs:
"In order not to overload the E-utility servers, NCBI recommends that users post no more than three URL requests per second and limit large jobs to either weekends or between 9:00 PM and 5:00 AM Eastern time during weekdays. "
  - https://www.ncbi.nlm.nih.gov/books/NBK25497/

*/

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	//"strconv"
	"strings"
	"time"
	//	"log"
)

const CHUNK_SIZE = 50000

const NUM_IDS_PER_URL = 50

//const NUM_IDS_PER_URL = 7
const AFTER_HOURS_MULTIPLYER = 1

var endPubmedArticleSet = "</PubmedArticleSet>"
var startPubmedArticleSet = "<PubmedArticleSet>"
var startXml = "<?xml version=\"1.0\"?>"
var docType = "<!DOCTYPE PubmedArticleSet PUBLIC \"-//NLM//DTD PubMedArticle, 1st January 2014//EN\" \"http://www.ncbi.nlm.nih.gov/corehtml/query/DTD/pubmed_140101.dtd\">"

var baseXmlFileName = "pubmed_xml_"

type ArticleSet struct {
	ArticleList []PubmedArticle `xml:"PubmedArticle"`
}

type PubmedArticle struct {
	MedlineCitation MedlineCitation `xml:"MedlineCitation"`
}

type MedlineCitation struct {
	PMID    string
	Article Article
	//Status string `xml:"Status,attr"`
	//Owner string `xml:"owner,attr"`
	MeshHeadingList MeshHeadingList
}

type Article struct {
	Abstract Abstract `xml:"Abstract"`
}
type Abstract struct {
	AbstractText AbstractText
}

type AbstractText struct {
	AbstractText string `xml:",chardata"`
}

type MeshHeadingList struct {
	MeshHeading []MeshHeading
}

type MeshHeading struct {
	DescriptorName DescriptorName
}

type DescriptorName struct {
	DescriptorName string `xml:",chardata"`
	MajorTopicYN   string `xml:"MajorTopicYN,attr"`
}

func main() {

	tr := &http.Transport{
		ResponseHeaderTimeout: time.Second * 500,
		DisableKeepAlives:     false,
		DisableCompression:    false,
	}

	//fileCount := 0
	/*
		xmlFile, err := os.Create("./" + baseXmlFileName + strconv.Itoa(fileCount) + ".gz")
		if err != nil {
			return
		}
		defer xmlFile.Close()
		ww := bufio.NewWriter(xmlFile)
		//wXml := gzip.NewWriter(ww)
	*/
	var wXml *gzip.Writer = nil
	var ww *bufio.Writer = nil
	var xFile *os.File = nil

	meshFile, err2 := os.Create("./pubmed.mesh.gz")
	if err2 != nil {
		return
	}
	defer meshFile.Close()
	wwMesh := bufio.NewWriter(meshFile)
	wMesh := gzip.NewWriter(wwMesh)

	//w := bufio.NewWriter(file)

	var pmids []string = make([]string, NUM_IDS_PER_URL*AFTER_HOURS_MULTIPLYER)

	allCount := 0
	count := 0
	reader := bufio.NewReader(os.Stdin)
	first := true
	chunkCount := 0
	for {
		numIdsPerUrl := findNumIdsPerUrl()
		line, err := reader.ReadString('\n')
		if err != nil {
			// You may check here if err == io.EOF
			break
		}
		line = strings.TrimSpace(line)
		//fmt.Println(line)
		pmids[count] = line

		if wXml == nil {
			wXml, ww, xFile = makeXmlWriter(allCount, pmids[0])
		}

		count = count + 1
		// Collected enough pmids: get their XML from NIH
		if count == numIdsPerUrl {
			getMesh(first, wMesh, wXml, tr, pmids)
			checkTime()
			first = false
			count = 0
			zeroArray(pmids)
		} else {

		}
		allCount += 1
		chunkCount += 1
		// Start new xml file: close old one: open new one
		if chunkCount > CHUNK_SIZE {
			fmt.Fprintln(wXml, endPubmedArticleSet)
			wXml.Flush()
			wXml.Close()
			ww.Flush()
			wXml, ww, xFile = makeXmlWriter(allCount, pmids[0])
			chunkCount = 0
			first = true
		}
		if allCount%500 == 0 {
			fmt.Println(allCount)
		}
	}
	if count != 0 {
		getMesh(first, wMesh, wXml, tr, pmids)
	}
	fmt.Fprintln(wXml, endPubmedArticleSet)

	wXml.Flush()
	wXml.Close()
	ww.Flush()

	wMesh.Flush()
	wMesh.Close()
	wwMesh.Flush()
	xFile.Close()
}

func zeroArray(a []string) {
	for i := 0; i < len(a); i++ {
		a[i] = ""
	}
}

func makeUrl(baseUrl string, pmids []string) string {
	url := baseUrl
	for i := 0; i < len(pmids); i++ {
		if pmids[i] != "" {
			if i != 0 {
				url += ","
			}
			url += pmids[i]
		}
	}
	//return "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&rettype=xml&id=15718680,15718682,119703,157186,1571868,11970375"
	return url
}

const baseUrl = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&rettype=xml&id="

func getMesh(first bool, meshWriter io.Writer, xmlWriter *gzip.Writer, transport *http.Transport, pmids []string) {
	preUrlTime := time.Now()
	url := makeUrl(baseUrl, pmids)

	fmt.Println(url)
	//fmt.Println(url)
	//fmt.Println("\n\n")
	client := &http.Client{Transport: transport}
	req, err := http.NewRequest("GET", url, nil)
	req.Close = true
	resp, err := client.Do(req)

	//resp, err := client.Get(url)
	if err != nil {
		fmt.Println("Error opening url:", url, "   error=", err)
		return
	}
	defer resp.Body.Close()
	//fmt.Println(err)

	//fmt.Println("\n\n")
	//fmt.Println(resp.Body)

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	s := buf.String()

	v := ArticleSet{}
	err = xml.Unmarshal([]byte(s), &v)

	//fmt.Println(v);
	// This should be inside a go routine listening on a channel
	for i := 0; i < len(v.ArticleList); i++ {
		//fmt.Println(i)
		fmt.Fprint(meshWriter, v.ArticleList[i].MedlineCitation.PMID)
		pubmedArticle := v.ArticleList[i]
		//fmt.Println(pubmedArticle.MedlineCitation.Article.Abstract.AbstractText)
		for j := 0; j < len(pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading); j++ {
			//fmt.Fprintln(meshWriter, " ", article.MedlineCitation.MeshHeadingList.MeshHeading[j].DescriptorName.DescriptorName)
			fmt.Fprint(meshWriter, "|")
			fmt.Fprint(meshWriter, pubmedArticle.MedlineCitation.MeshHeadingList.MeshHeading[j].DescriptorName.DescriptorName)
		}
		fmt.Fprintln(meshWriter, "")
	}
	if !first {
		s = strings.Replace(s, startXml, "", -1)
		s = strings.Replace(s, docType, "", -1)
		s = strings.Replace(s, startPubmedArticleSet, "", -1)
	}
	s = strings.Replace(s, endPubmedArticleSet, "<!-- breakset -->", -1)

	xmlWriter.Write([]byte(s))
	postUrlTime := time.Now()
	fmt.Println("Total request time:", postUrlTime.Sub(preUrlTime))
}

func makeXmlWriter(fileCount int, startPmid string) (*gzip.Writer, *bufio.Writer, *os.File) {
	//xmlFile, err := os.Create("./" + baseXmlFileName + strconv.Itoa(fileCount) + "_" + strconv.Itoa(fileCount+CHUNK_SIZE) + ".gz")
	xmlFile, err := os.Create("./" + baseXmlFileName + startPmid + ".gz")
	if err != nil {
		return nil, nil, nil
	}
	ww := bufio.NewWriter(xmlFile)
	return gzip.NewWriter(ww), ww, xmlFile
}

func afterHours() bool {
	now := time.Now()
	hour, _, _ := now.Clock()
	return hour < 8 || hour > 18
}

func findNumIdsPerUrl() int {
	if afterHours() {
		return NUM_IDS_PER_URL * AFTER_HOURS_MULTIPLYER
	}
	return NUM_IDS_PER_URL
}

func checkTime() {
	now := time.Now()
	rand.Seed(int64(now.Nanosecond()/9999999 + now.Second()*100 + now.Hour()))
	sleepSeconds := 1 + rand.Intn(2)

	/*
		if !afterHours() {
			sleepSeconds = 1 + rand.Intn(2)
		}
	*/

	duration := (time.Duration)(sleepSeconds)
	fmt.Println("Start sleep")
	t0 := time.Now()

	time.Sleep(duration * time.Second)
	t1 := time.Now()
	fmt.Println("End sleep:", t1.Sub(t0))
}
