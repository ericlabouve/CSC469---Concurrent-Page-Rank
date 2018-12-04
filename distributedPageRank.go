// Distributed Page Rank
// Idea:
// Split the graph by domain addresses.
// Then run page rank on the local cluster.
// Report URLs with highest page rank scores for each domain and compare with seq. results.

package main

import (
	"math"
	"fmt"
	"os"
	"bufio"
	"log"
	"strings"
	"sort"
	"sync"
	"time"
)

var wg sync.WaitGroup

// Structure hold all information for each subgraph 
type Subgraph struct {
	// Name of the domain
	domainName string
	// List of all the nodes
	nodes []string
	// Maps a node to a list of incoming nodes
	adjacencyList map[string][]string
	// Maps a node to its number of outlinks
	outLinks map[string]int
	// Old page rank values
	pageRankOld map[string]float32
	// New page rank values
	pageRankNew map[string]float32
}

func newSubgraph() *Subgraph {
	g := new(Subgraph)
	g.adjacencyList = make(map[string][]string)
	g.outLinks = make(map[string]int)
	g.pageRankOld = make(map[string]float32)
	g.pageRankNew = make(map[string]float32)
	return g
}

type Pair struct {
	url string
	pageRank float32
}

// Print the nodes with the top 'num' page rank scores for testing
// Results are compared against a java implementation on the same dataset
func printTop(subGraph *Subgraph, num int) {
	tupleList := []Pair{}
	for k, v := range subGraph.pageRankNew {
		tupleList = append(tupleList, Pair{k, v})
	}
	sort.Slice(tupleList, func(i, j int) bool {
  		return tupleList[i].pageRank > tupleList[j].pageRank
	})
	for i:=0; i<num; i++ {
		p := tupleList[i]
		fmt.Printf("(%s, %f)", p.url, p.pageRank)
	}
}

// Calculates the L1 norm between the two vectors
// If x=[x1,...xn] and y=[y1,...,yn] are vectors, 
// the L1 norm of their distance is equal to |x1-y1|+...+|xn-yn|
func distance(pageRankOld map[string]float32, pageRankNew map[string]float32) float32 {
	// Error check
	if len(pageRankOld) != len(pageRankNew) {
		return math.MaxFloat32
	}
	distance := float32(0)
	for k, v := range pageRankOld {
		distance += float32(math.Abs(float64(v - pageRankNew[k])))
	}
	return distance
}

// Copies the content of the map into a new map
func deepCopyMap(m map[string]float32) map[string]float32 {
	newMap := map[string]float32{}
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

// Random Click Probability
// The probability that a user will reach this URL if a user selects a 
// node from the graph at random
func randClickProb(subGraph *Subgraph, d float32) float32 {
	return (1 - d) * (float32(1) / float32(len(subGraph.nodes)))
}

// Prestige = a measure of how important a node is by counting its incoming edges
// The probability that a user will reach this URL from another URL
func hyperLinkClick(subGraph *Subgraph, node string, d float32) float32 {
	prestige := float32(0)
	// Nodes that do not have any in-edges have a prestige of zero
	if _, ok := subGraph.adjacencyList[node]; ok {
		// For each node that has an edge pointing to the current node
		for _, inNode := range subGraph.adjacencyList[node] {
			// Will never divide by zero since inNode points to node
			prestige += subGraph.pageRankOld[inNode] / float32(subGraph.outLinks[inNode])
		}
	}
	return d * prestige
}

// Normalize values in the map to sum to one
// Normalize because we want the sum of probabilities to equal one
func normalizePageRankNew(subGraph *Subgraph) {
	sum := float32(0)
	for _, v := range subGraph.pageRankNew {
		sum += v
	}
	for k, v := range subGraph.pageRankNew {
		subGraph.pageRankNew[k] = v / sum
	}
}

// The following equation is used to calculate the rank for a single node:
// p(i) = (1-d)*(1/|V|) + d*SUM{0...k}[(1/|Oj|) * p(j)]
//		  Random Click  + Prestige
//
//	d = Probability that weights influence of Random Click and Prestige (Set to 0.9)
//	|V| = Number of nodes in the graph
//  {0...k} = Nodes that have edges pointing to node i
//  j = A node that has an edge pointing to node i
//	|Oj| = Number of outlinks from node j.
//	p(j) = The page rank for node j
//
// This equation states that the probability of visiting a node, i, is the sum of:
// 		1. A random click probability
//		2. The prestige of node, i.
func pageRank(subGraph *Subgraph, d float32, epsilon float32) {
	// Continue to calculate page rank until a minimum threshold is reached
	// The threshold is a measure of the graph's change, so we quit when the
	// the graph stops changing.
	for {
		subGraph.pageRankOld = deepCopyMap(subGraph.pageRankNew)
		// Calculate page rank for each URL
		for _, url := range subGraph.nodes {
			randomClick := randClickProb(subGraph, d);
			hyperLinkClick := hyperLinkClick(subGraph, url, d);
			subGraph.pageRankNew[url] = randomClick + hyperLinkClick
		}
		// Normalize because we want the sum of probabilities to equal one
		normalizePageRankNew(subGraph)
		if distance(subGraph.pageRankOld, subGraph.pageRankNew) < epsilon {
			break
		}
	}
}


// Normalizes all page rank values to sum to one
func initPageRank(subGraph *Subgraph) {
	numNodes := len(subGraph.nodes)
	for _, node := range subGraph.nodes {
		subGraph.pageRankNew[node] = float32(1)/float32(numNodes)
	}
}

// Initializes and computes the page rank of the subgraph
func localizedPageRank(subGraph *Subgraph) {
	initPageRank(subGraph)
	pageRank(subGraph, 0.9, 0.0001)
	wg.Done()
}


// Reads the dot file and fills out:
// 	  1. nodes
//    2. adjacencyList
//	  3. outLinks
func readDotFileByDomain(path string, domain string) *Subgraph {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// Map to keep track if we have seen node before
	visitedURL := make(map[string]bool)
	subgraph := newSubgraph()
	subgraph.domainName = domain
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), "->")
		if len(s) == 2 {
			src := strings.TrimSpace(s[0])
			// CHECK IF THE SOURCE LINK IS PART OF THE DOMAIN
			if strings.Contains(src, domain+".calpoly.edu") {
				dest := strings.TrimSpace(strings.Replace(s[1], ";", "", -1))
				// Add to nodes list if we have not come across this url before
				if _, ok := visitedURL[src]; !ok {
					visitedURL[src] = true
					subgraph.nodes = append(subgraph.nodes, src)
					subgraph.outLinks[src] = 0
				}
				if _, ok := visitedURL[dest]; !ok {
					visitedURL[dest] = true
					subgraph.nodes = append(subgraph.nodes, dest)
				}
				// Add to adjacencyList
				if _, ok := subgraph.adjacencyList[dest]; !ok {
					subgraph.adjacencyList[dest] = make([]string, 0)
				}
				subgraph.adjacencyList[dest] = append(subgraph.adjacencyList[dest], src)
				
				// Add to outLinks
				subgraph.outLinks[src]++
			}
		}		
	}
	return subgraph
}


func isDomain(src, domain string) bool {
	if strings.Contains(src, domain+".calpoly.edu") {
		return true
	} else {
		return false
	}
}


// Loops through each URL and saves a copy of the string that
// resides in the location in between http:// and calpoly.edu
// For example, http://ceng.calpoly.edu/ will save "ceng"
func getDomains(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	// Map to keep track if we have seen a domain before
	visitedDomain := make(map[string]bool)
	visitedDomain[""] = true  // http://calpoly.edu/
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), "->")
		if len(s) == 2 {
			src := strings.TrimSpace(s[0])
			domains := strings.Split(src, string('.'))
			// Get location of "calpoly" and use the domain immediately before it
			for idx, domainStr := range domains {
				if "calpoly" == domainStr && idx > 0 {
					domain := strings.Replace(domains[idx - 1], "https://", "", -1)
					domain = strings.Replace(domain, "http://", "", -1)
					visitedDomain[domain] = true
				}
			}
		}
	}
	domains := make([]string, 0, len(visitedDomain))
    for domain := range visitedDomain {
        domains = append(domains, domain)
    }
    return domains
}

func combineSubgraphs(subgraphs []*Subgraph) *Subgraph {
	globalGraph := newSubgraph()
	visitedURL := make(map[string]bool)
	// Merge subgraph elements
	for _, subgraph := range subgraphs {
		// Combine the nodes list
		for _, url := range subgraph.nodes {
			if isDomain(url, subgraph.domainName) {
				if _, ok := visitedURL[url]; !ok { 
					visitedURL[url] = true
					globalGraph.nodes = append(globalGraph.nodes, url)
				}
			}
		}
	}
	visitedURL = make(map[string]bool)
	for _, subgraph := range subgraphs {
		// Combine the adjecencyList map
		for url, value := range subgraph.adjacencyList {
			if isDomain(url, subgraph.domainName) {
				if _, ok := visitedURL[url]; !ok { 
					visitedURL[url] = true
					globalGraph.adjacencyList[url] = value
				}
			}
		}
	}
	visitedURL = make(map[string]bool)
	for _, subgraph := range subgraphs {
		// Combine the outLinks map
		for url, value := range subgraph.outLinks {
			if isDomain(url, subgraph.domainName) {
				if _, ok := visitedURL[url]; !ok { 
					visitedURL[url] = true
					globalGraph.outLinks[url] = value
				}
			}
		}
	}
	visitedURL = make(map[string]bool)
	for _, subgraph := range subgraphs {
		// Combine the pageRankNew map
		for url, value := range subgraph.pageRankNew {
			if isDomain(url, subgraph.domainName) {
				if _, ok := visitedURL[url]; !ok { 
					visitedURL[url] = true
					globalGraph.pageRankNew[url] = value
				}
			}
		}
	}
	return globalGraph
}


// Would like to time just the page rank execution times
func main() {
	dotFile := "./dot_files/auth.gv"
	// Split URLs by domain
	domains := getDomains(dotFile)
	// Array to hold pointers to subgraphs for each goroutine
	subgraphs := make([]*Subgraph, len(domains))
	// Read dot files by domain
	for idx, domain := range domains {
		subgraph := readDotFileByDomain(dotFile, domain)
		subgraphs[idx] = subgraph
	}

	start := time.Now()

	count := 0
	// Launch a new goroutine for each subgraph
	for _, subGraphPtr := range subgraphs {
		wg.Add(1)
		count++
		go localizedPageRank(subGraphPtr)
	}
	wg.Wait()
	// Print top URL from each subgraph
	// for _, subgraph := range subgraphs {
	// 	fmt.Printf("\n%s:\t", subgraph.domainName)
	// 	printTop(subgraph, 1)
	// }

	copyTime := time.Now()

	// Combine subgraphs into a global graph
	globalGraph := combineSubgraphs(subgraphs)

	// Removes time for copying over datastructures
	// This time can be igored because we are working
	// in the same memory space
	copyTimeElapsed := time.Since(copyTime)

	// Run sequential PR on global graph
	normalizePageRankNew(globalGraph)
	pageRank(globalGraph, 0.9, 0.0001)	

	elapsed := time.Since(start)
	fmt.Printf("Concurrent Time = %s\n", elapsed - copyTimeElapsed)
}
