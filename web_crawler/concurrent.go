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

	"CPE469/lab1/links"
)

//!+breadthFirst
// breadthFirst calls f for each item in the worklist.
// Any items returned by f are added to the worklist.
// f is called at most once for each item.
func breadthFirst(f func(c chan int, c_str chan string, item string), depth int, worklist []string) {
	seen := make(map[string]bool)
	//for len(worklist) > 0 {
	c := make(chan int)
	c_str := make(chan string)
	for i := 0; i < depth; i++ {
		items := worklist
		worklist = nil
		length := len(items) //length of inner loop to know number of items to get channel response from 
		for _, item := range items {
			if !seen[item] {
				seen[item] = true
				go f(c, c_str, item) // concurrent call, pass in channel to get number of strings and then channel to get the strings
				
			} else {
				length-- // subtract if a duplicate because wont run crawl function on
			}
		}

		for i := 0; i < length; i++ {
			ret_len := <- c // get number of strings to be returned
			for j := 0; j < ret_len; j++ {
				val := <- c_str // get back urls
				worklist = append(worklist, val) // append new url to worklist
			}
			
		}
	}


}

//!-breadthFirst

//!+crawl
func crawl(c chan int, c_str chan string, url string) {
	fmt.Println(url)
	list, err := links.Extract(url)
	if err != nil {
		log.Print(err)
	}
	c <- len(list) // before sending list of urls, send how many will be sent

	for _, str := range list {
		c_str <- str // for each url send through channel
	}
}

//!-crawl

//!+main
func main() {
	// Crawl the web breadth-first,
	// starting from the command-line arguments.
	depthPtr := flag.Int("depth", 3, "url crawler limit")
	flag.Parse()

	fmt.Println(*depthPtr)

	start := time.Now()
	breadthFirst(crawl, *depthPtr, os.Args[2:])
	elapsed := time.Since(start).Seconds()
	fmt.Printf("Time elapsed: %.2fs\n", elapsed)
}

//!-main

