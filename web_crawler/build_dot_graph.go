// Copyright © 2016 Thw Go Programming Language
// License: https://creativecommons.org/licenses/by-nc-sa/4.0/


// Findlinks3 crawls the web, starting with the URLs on the command line.
package main

import (
	"fmt"
	"log"
	"os"
	"flag"
	"time"
	"bufio"
	"./links"
)

var calpoly_url string = "https://www.calpoly.edu"
var usr string
var pass string

//!+createFile
// create a file given filename
func createFile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println("File couldnt be created")
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	writer.WriteString("digraph {\n")
	writer.Flush()
	f.Close()
}

//!-createFile

//!+writeToFile
// write to .gv file with origin_url and all the urls it points to
func writeToFile(fp *os.File, origin_url string, url_list []string) {
	writer := bufio.NewWriter(fp)
	
	for _, url := range url_list {
		str := fmt.Sprintf("%s -> %s;\n", origin_url, url) // write each link with original url and the new url it links
		writer.WriteString(str)
	}

	writer.Flush()
}

//!-writeToFile

//!+breadthFirst
// breadthFirst calls f for each item in the worklist.
// Any items returned by f are added to the worklist.
// f is called at most once for each item.
func breadthFirst(f func(item string) []string, fp *os.File, worklist []string) {
	seen := make(map[string]bool)
	c := make(chan []string) // channel to send list of discovered urls
	u := make(chan string) // channel to send original url

	count := 0
	for len(worklist) > 0 {
		items := worklist
		worklist = nil
		length := 0 //length of inner loop to know number of items to get channel response from

		g := func(url string) {
			c <- f(url) // send returned list of new urls
			u <- url // send url the list came from
		}

		for _, item := range items {
			if !seen[item] {
				length++
				seen[item] = true
				go g(item) // concurrent call to nested function g
			}
		}

		for i := 0; i < length; i++ {
			val := <- c // get back urls
			origin := <- u // get origin url since order not guarunteed
			worklist = append(worklist, val...) // append new url to worklist
			writeToFile(fp, origin, val) // write new connections to file in form "origin -> url"
		}

	}


}

//!-breadthFirst

//!+crawl
func crawl(url string) []string {
	fmt.Println(url)
	list, err := links.Extract(url, usr, pass)
	if err != nil {
		log.Print(err)
	}
	return list
}

//!-crawl

//!+main
func main() {
	// Crawl the web breadth-first,
	// starting from the command-line arguments.
	urls := []string{calpoly_url}
	filename := flag.String("f", "calpoly.gv", "name of file to create")
	flag.StringVar(&usr, "u", "", "calpoly username")
	flag.StringVar(&pass, "p", "", "calpoly password")
	flag.Parse()
	fmt.Printf("Writing to file dot_files/%s\n", *filename)
	filepath := fmt.Sprintf("../dot_files/%s", *filename)
	createFile(filepath)

	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error opening file for write and append")
		fmt.Println(err)
		return
	}
	defer f.Close()

	// list := crawl(calpoly_url)
	// fmt.Println(len(list))
	// for _, url := range list {
	// 	fmt.Println(url)
	// }
	// return
	start := time.Now()
	fmt.Println("Starting web crawler...")
	breadthFirst(crawl, f, urls)
	elapsed := time.Since(start).Seconds()

	writer := bufio.NewWriter(f)
	writer.WriteString("}\n")
	writer.Flush()
	fmt.Println("Web crawler complete")

	fmt.Printf("Time elapsed: %.2fs\n", elapsed)
}

//!-main

