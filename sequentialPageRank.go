package main

import (
	"math"
	"fmt"
	"os"
	"bufio"
	"log"
	"strings"
	"sort"
	"time"
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

type Pair struct {
	url string
	pageRank float32
}

// Print the nodes with the top 20 page rank scores for testing
// Results are compared against a java implementation on the same dataset
func printTop20() {
	tupleList := []Pair{}
	for k, v := range pageRankNew {
		tupleList = append(tupleList, Pair{k, v})
	}
	sort.Slice(tupleList, func(i, j int) bool {
  		return tupleList[i].pageRank > tupleList[j].pageRank
	})
	fmt.Printf("Top 20:\n")
	for i:=0; i<20; i++ {
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
func randClickProb(d float32) float32 {
	return (1 - d) * (float32(1) / float32(len(nodes)))
}

// Prestige = a measure of how important a node is by counting its incoming edges
// The probability that a user will reach this URL from another URL
func hyperLinkClick(node string, d float32) float32 {
	prestige := float32(0)
	// Nodes that do not have any in-edges have a prestige of zero
	if _, ok := adjacencyList[node]; ok {
		// For each node that has an edge pointing to the current node
		for _, inNode := range adjacencyList[node] {
			// Will never divide by zero since inNode points to node
			prestige += pageRankOld[inNode] / float32(outLinks[inNode])
		}
	}
	return d * prestige
}

// Normalize values in the map to sum to one
// Normalize because we want the sum of probabilities to equal one
func normalizePageRankNew() {
	sum := float32(0)
	for _, v := range pageRankNew {
		sum += v
	}
	for k, v := range pageRankNew {
		pageRankNew[k] = v / sum
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
func pageRank(d float32, epsilon float32) {
	// Continue to calculate page rank until a minimum threshold is reached
	// The threshold is a measure of the graph's change, so we quit when the
	// the graph stops changing.
	for {
		pageRankOld = deepCopyMap(pageRankNew)
		// Calculate page rank for each URL
		for _, url := range nodes {
			randomClick := randClickProb(d);
			hyperLinkClick := hyperLinkClick(url, d);
			pageRankNew[url] = randomClick + hyperLinkClick
		}
		// Normalize because we want the sum of probabilities to equal one
		normalizePageRankNew()
		if distance(pageRankOld, pageRankNew) < epsilon {
			fmt.Printf("Done\n")
			break
		}
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
		}		
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

func printTopDomains() {
	dotFile := "./dot_files/auth.gv"
	// Split URLs by domain
	domains := getDomains(dotFile)

	tupleList := []Pair{}
	for k, v := range pageRankNew {
		tupleList = append(tupleList, Pair{k, v})
	}
	sort.Slice(tupleList, func(i, j int) bool {
  		return tupleList[i].pageRank > tupleList[j].pageRank
	})

	// Map to keep track if we have seen domain before
	visitedDomain := make(map[string]bool)
	for _, domain := range domains {
		visitedDomain[domain] = false
	}
	// Loop through sorted list until we have gathered all top domains
	count := 0
	for i:=0; count < len(domains); i++ {
		// Get the URL's domain
		pieces := strings.Split(tupleList[i].url, string('.'))
		// Get location of "calpoly" and use the domain immediately before it
		for idx, domainStr := range pieces {
			if "calpoly" == domainStr && idx > 0 {
				domain := strings.Replace(pieces[idx - 1], "https://", "", -1)
				domain = strings.Replace(domain, "http://", "", -1)
				if visitedDomain[domain] == false {
					visitedDomain[domain] = true
					count++
					fmt.Printf("%s:\t(%s, %f)\n", domain, tupleList[i].url, tupleList[i].pageRank)	
				}
				
			}
		}
	}

	fmt.Printf("Top 20:\n")
	for i:=0; i<20; i++ {
		p := tupleList[i]
		fmt.Printf("(%s, %f)", p.url, p.pageRank)
	}
}

func main() {
	// Read in dot graph
	readDotFile("./dot_files/auth.gv")
	start := time.Now()
	// Normalize initialize starting page rank values
	initPageRank()
	// Execute the sequential page rank algorithm
	pageRank(0.9, 0.0001)
	elapsed := time.Since(start)
	fmt.Printf("Linear Time = %s\n", elapsed)
	// Testing purposes
	// printTop20()
	// printTopDomains()
}

