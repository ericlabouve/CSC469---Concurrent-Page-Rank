package main

import (
	//"math/rand"
	"fmt"
	"os"
	"bufio"
	"log"
	"strings"
	// "regexp"
	//"time"
)

// List of all the nodes
var nodes []string
// Maps a node to a list of incoming nodes
var adjacencyList = map[string][]string{}
// Maps a node to its number of outlinks
var outLinks = map[string]int{}
// Old page rank values
var pageRankOld = map[string]float32{}
// New page rank values
var pageRankNew = map[string]float32{}

func printGraph() {
	for _, node := range nodes {
		fmt.Printf("%s outlinks: %d\n", node, outLinks[node])
	}

	for k, v := range adjacencyList { 
		s := ""
		for _, node := range v {
			s += node + ", "
		}
		fmt.Printf("%s inlinks: %s\n", k, s)
	}
}

// Normalizes all page rank values to sum to one
func initPageRank() {
	numNodes := len(nodes)
	for _, node := range nodes {
		pageRankNew[node] = float32(1)/float32(numNodes)
	}
}

// Reads the dot file and fills out:
// 	  1. nodes
//    2. adjacencyList
//	  3. outLinks
func readDotFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// Map to keep track if we have seen node before
	visitedURL := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), "->")
		if len(s) == 2 {
			src := strings.TrimSpace(s[0])
			dest := strings.TrimSpace(strings.Replace(s[1], ";", "", -1))
			// Add to nodes list if we have not come across this url before
			if _, ok := visitedURL[src]; !ok {
				visitedURL[src] = true
				nodes = append(nodes, src)
				outLinks[src] = 0
			}
			if _, ok := visitedURL[dest]; !ok {
				visitedURL[dest] = true
				nodes = append(nodes, dest)
			}
			// Add to adjacencyList
			if _, ok := adjacencyList[dest]; !ok {
				adjacencyList[dest] = make([]string, 0)
			}
			adjacencyList[dest] = append(adjacencyList[dest], src)
			
			// Add to outLinks
			outLinks[src]++

			if count == 10 {
				break
			}
			count++
		}		
	}
	printGraph()
}

func main() {
	// Read in dot graph
	readDotFile("./dot_files/graph.gv")
	// Initialize starting page rank values
	initPageRank()
}