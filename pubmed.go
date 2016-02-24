package main

const endPubmedArticleSet = "</PubmedArticleSet>"
const startPubmedArticleSet = "<PubmedArticleSet>"
const startXml = "<?xml version=\"1.0\"?>"
const docType = "<!DOCTYPE PubmedArticleSet PUBLIC \"-//NLM//DTD PubMedArticle, 1st January 2014//EN\" \"http://www.ncbi.nlm.nih.gov/corehtml/query/DTD/pubmed_140101.dtd\">"


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
