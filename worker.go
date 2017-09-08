/*
Implements the worker(s) in assignment 4

Usage:

go run worker.go [server ip:port]

*/

package main

import (
	"fmt"
	"golang.org/x/net/html"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type InitRes struct {
	WorkerIP string
}

type WWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

type GetWorkerReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

type GoCrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

type Node struct {
	Parent string
	//Domain string
	Children      []string
	mapOfChildren map[string]Node
}

type Domains struct {
	mapOfDomains map[string]Node
}

type RedirectReq struct {
	DomainLink string
	NewLink    string
	Dep        int
}

type WorkerOverlapReq struct {
	StartURL string
}

// Response to MServer.Overlap
type WorkerOverlapRes struct {
	Pars    string
	Childs  []string
	MChilds map[string]Node
	Outer   []string
}

type SendBack struct {
	ListOut      []string
	ParentNode   string
	ChildrenNode []string
	ChildsNode   map[string]Node
}

type WServer int

var ListOfTimes []int
var serverIP string
var ListOfDomains []string

var newMapOfDomains = map[string]Node{}

func main() {
	serverIP, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	done := make(chan int)

	// get worker IP address
	client, err := rpc.Dial("tcp", serverIP)
	checkError("Server Dial:", err, true)
	var req bool
	var res string
	err = client.Call("SServer.GetIPaddress", req, &res)
	checkError("GetIPaddress: ", err, true)

	port := res
	client.Close()

	go func() {
		wServer := rpc.NewServer()
		w := new(WServer)
		wServer.Register(w)

		lw, err := net.Listen("tcp", port)
		checkError("Listen workers: ", err, true)
		for {

			connW, err := lw.Accept()
			checkError("Accept client: ", err, false)
			go wServer.ServeConn(connW)
		}
	}()

	<-done
}

func (w *WServer) StartCrawl(args *GoCrawlReq, reply *bool) error {
	fmt.Println("Initial MAP: ", newMapOfDomains)
	err := StartCrawlHandler(args.URL, args.Depth)
	//TODO: Append Nodes, check the Parent

	checkError("GoCrawlHandler: ", err, false)
	*reply = true
	fmt.Println("Worker finished Crawl")
	return nil
}

func StartCrawlHandler(url string, depth int) error {
	removeHttp := strings.TrimPrefix(url, "http://")
	splitDomain := strings.Split(removeHttp, "/")
	domain := splitDomain[0]

	// List of Domains this worker owns
	r := false
	for _, i := range ListOfDomains {
		if i == domain {
			fmt.Println("Already in list of domains")
			r = true
			break
		}
	}

	if !r {
		ListOfDomains = append(ListOfDomains, domain)
	}

	fmt.Println("Current Domains: ", ListOfDomains)

	// node struct that stores the domain, URL of a page, and
	// a collection of pointers to other nodes that are its children

	// TODO: Undo?
	//newMapOfDomains := map[string]Node{}

	result := htmlParse(url, depth, domain)

	newMapOfDomains[domain] = result

	fmt.Println("PART MAP: ", newMapOfDomains)
	return nil
}

// recursively call
func htmlParse(urlstring string, depth int, domain string) Node {
	// input: url to http get --> parse
	// output: links to go routine recursively call, save links
	newChildren := make(map[string]Node)
	listofChildren := []string{}
	newNode := Node{urlstring, listofChildren, newChildren}

	newDepth := depth
	resp, error := http.Get(urlstring)
	//checkError("HTTP Get: ", err, false)
	defer resp.Body.Close()
	z := html.NewTokenizer(resp.Body)

	//fmt.Println("CURRENT DEPTH: ", newDepth)

	if (newDepth < 1) || (error != nil) {
		return newNode
	}

	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			fmt.Println("End of document")
			return newNode
		case html.StartTagToken:
			tn, _ := z.TagName()
			if len(tn) == 1 && string(tn) == "a" {
			AttrLoop:
				for {
					k, val, m := z.TagAttr()
					if string(k) == "href" {
						rawLink := string(val)
						//fmt.Println("RAWLINK: ", rawLink)

						u, err := url.Parse(rawLink)
						checkError("Parse Ref: ", err, true)
						base, err := url.Parse(urlstring)
						checkError("Parse Base: ", err, true)

						newLink := base.ResolveReference(u).String()

						if strings.HasSuffix(newLink, ".html") && !strings.HasPrefix(newLink, "mailto:") && !strings.HasPrefix(newLink, "https://") && strings.HasPrefix(newLink, "http://") {

							// u, err := url.Parse(rawLink)
							// checkError("Parse Ref: ", err, true)
							// base, err := url.Parse(urlstring)
							// checkError("Parse Base: ", err, true)

							// newLink := base.ResolveReference(u).String()
							//fmt.Println("Possible NewLink: ", newLink)

							if _, ok := newChildren[newLink]; ok {
								fmt.Println("Already exists: ", newLink)
								break AttrLoop
							}

							removeHttp := strings.TrimPrefix(newLink, "http://")
							splitLink := strings.Split(removeHttp, "/")
							newSplitLink := splitLink[0]
							//fmt.Println("Domain of NewLink: ", newSplitLink)

							listofChildren = append(listofChildren, newLink)
							newNode.Children = listofChildren

							//fmt.Println(ListOfDomains)
							r := false
							for _, i := range ListOfDomains {
								if i != newSplitLink {
									// domain is not part of this worker to map
									continue
								}
								// domain is in this worker
								r = true
							}

							if r {
								newChildren[newLink] = Node{}
								newNode.mapOfChildren = newChildren
							} else {
								fmt.Println("Domain not for this worker ", newSplitLink)

								RedirectCrawl(newSplitLink, newLink, newDepth-1)
								break AttrLoop

							}

							if newDepth > 1 {
								fmt.Println("Depth is not zero, keep going:", newDepth, "Start Parsing: ", newLink)

								if !r {
									fmt.Println("FINALLY HERE")
									fmt.Println("Domain not for this worker ", newSplitLink)

									RedirectCrawl(newSplitLink, newLink, newDepth-1)
									break AttrLoop
								}

								fmt.Println("Domain IS for this worker ", newSplitLink)

								c := htmlParse(newLink, newDepth-1, domain)

								fmt.Println("C Value:", c)

								newChildren[newLink] = c
								newNode.mapOfChildren = newChildren
							}

						}

					}

					if !m {
						break AttrLoop
					}
				}
			}
		}
	}

	fmt.Println("done parsing at all depths")
	return newNode
}

func RedirectCrawl(domain string, newURL string, dep int) {
	args := os.Args[1:]
	serverIP := args[0]

	serv, err := rpc.Dial("tcp", serverIP)
	checkError("Redirect Dial:", err, true)
	Rargs := RedirectReq{
		DomainLink: domain,
		NewLink:    newURL,
		Dep:        dep,
	}
	var reply bool
	error := serv.Call("SServer.Redirect", Rargs, &reply)
	checkError("Redirect Call:", error, true)
}

func (w *WServer) GetFullMap(args *WorkerOverlapReq, reply *WorkerOverlapRes) error {
	fmt.Println("Current Full Map: ", newMapOfDomains)
	// Parse the full Map to the point where StartURL starts.
	p, c, mc, out, err := GetFullMapHandler(args.StartURL)
	checkError("GetFullMapHandler Error: ", err, true)

	reply.Pars = p
	reply.Childs = c
	reply.MChilds = mc
	reply.Outer = out

	fmt.Println("Worker finished GetFullMap")
	return nil
}

func (w *WServer) GoCompare(args *SendBack, reply *int) error {
	var NodeOfWorker Node
	NodeOfWorker = Node{args.ParentNode, args.ChildrenNode, args.ChildsNode}
	fmt.Println("CurrentNode to Compare: ", NodeOfWorker)
	i := compare(args.ListOut, NodeOfWorker)

	*reply = i
	fmt.Println("Worker finished GoCompare")

	return nil
}

func compare(ListOfOuter []string, Graph Node) int {
	laps := 0
	// Subtract it from the List
	// Check parent
	for _, x := range ListOfOuter {
		//fmt.Println(Graph.Parent)
		if x == Graph.Parent {
			fmt.Println("Points to Parent, add 1")
			laps++
			break // TODO: COMMENT OUT
		}
	}
	// Check Children
	for i := 0; i < len(ListOfOuter); i++ {
		for j := 0; j < len(Graph.Children); j++ {
			fmt.Println("Comparing", ListOfOuter[i], "to :", Graph.Children[j])
			if ListOfOuter[i] == Graph.Children[j] {
				fmt.Println("Points to Children, add 1")
				laps++

			}
		}
	}
	// Check Children's Children
	for _, v := range Graph.mapOfChildren {
		if len(v.Children) != 0 {
			compare(ListOfOuter, v)

		}
	}

	return laps

}

func GetFullMapHandler(startLink string) (string, []string, map[string]Node, []string, error) {

	p, c, mc := recursiveGet(startLink, newMapOfDomains)
	parsedNode := Node{p, c, mc}
	fmt.Println("Parsed Node: ", parsedNode)
	ListOfOuter := checkChildren(parsedNode, p)

	fmt.Println("ListOfOuter: ", ListOfOuter)

	return p, c, mc, ListOfOuter, nil
}

func checkChildren(firstTree Node, mainTreeDomain string) []string {
	//search firstTree for external links
	removeHttp := strings.TrimPrefix(mainTreeDomain, "http://")
	split1 := strings.Split(removeHttp, "/")
	firstTreeDomain := split1[0]

	Outerlinks := []string{}

	//Outerlinks = append(Outerlinks, firstTree.Parent)

	for i := 0; i < len(firstTree.Children); i++ {
		removeChild := strings.TrimPrefix(firstTree.Children[i], "http://")
		split2 := strings.Split(removeChild, "/")
		s := split2[0]
		fmt.Println("Comparing ", s, "to Main: ", firstTreeDomain)
		if s != firstTreeDomain {
			fmt.Println("Appending: ", firstTree.Children[i])
			Outerlinks = append(Outerlinks, firstTree.Children[i])
		}
	}

	fmt.Println("Current Outerlinks: ", Outerlinks)

	for _, v := range firstTree.mapOfChildren {
		if len(v.Children) != 0 {
			no := checkChildren(v, mainTreeDomain)
			Outerlinks = append(Outerlinks, no...)
		}
	}
	return Outerlinks
}

func recursiveGet(startLink string, currentMap map[string]Node) (string, []string, map[string]Node) {
	// removeHttp := strings.TrimPrefix(startLink, "http://")
	// splitHttp := strings.Split(removeHttp, "/")
	// startLinkDomain := splitHttp[0]

	//r := false
	a := ""
	b := []string{}
	c := make(map[string]Node)
	currentNode := Node{a, b, c}
	for _, v := range currentMap {
		//if k == startLinkDomain {
		currentNode = v
		//r = true

		if currentNode.Parent == startLink {
			fmt.Println("Found parent, taking this Node")
			return currentNode.Parent, currentNode.Children, currentNode.mapOfChildren
		} else {
			if currentNode.mapOfChildren != nil {
				fmt.Println("Didn't find parent, keep looking")
				fmt.Println("MAPOFCHILREN: ", currentNode.mapOfChildren)
				par, ch, mc := recursiveGet(startLink, currentNode.mapOfChildren)
				a = par
				b = ch
				c = mc
			}
		}

		//}
	}

	//fmt.Println("CurrentNode: ", currentNode)
	//    if r {
	//     if currentNode.Parent == startLink {
	//     	fmt.Println("Found parent, taking this Node")
	//     	return currentNode.Parent, currentNode.Children, currentNode.mapOfChildren
	//     } else {
	//     	if currentNode.mapOfChildren != nil {
	// 	    	fmt.Println("Didn't find parent, keep looking")
	// 	    	fmt.Println("MAPOFCHILREN: ", currentNode.mapOfChildren)
	// 		   	recursiveGet(startLink, currentNode.mapOfChildren)
	// 	   	} else {
	// 	   		return currentNode.Parent, currentNode.Children, currentNode.mapOfChildren
	// 	   	}
	// 	}
	// } else {
	// 	// Did not find at first For loop...
	// 	for _, v := range currentMap {
	// 		fmt.Println("Not Found on first level, search each Key deeper")
	// 		recursiveGet(startLink, v.mapOfChildren)
	//     }
	// }

	fmt.Println("Start URL does not exist in this Map")
	return a, b, c
}

func (w *WServer) GetDomains(args *bool, reply *[]string) error {
	fmt.Println("WORKER DOMAINS: ", ListOfDomains)
	*reply = ListOfDomains
	fmt.Println("Worker finished GetDomains")
	return nil
}

func (w *WServer) GetDomainsForRedirect(args string, reply *bool) error {
	//fmt.Println("Checking against this Domain: ", args)
	for _, i := range ListOfDomains {
		if i == args {
			fmt.Println("I have this domain: ", args)
			*reply = true
			return nil
		}
	}
	fmt.Println("I don't  have this domain: ", args)
	*reply = false

	return nil
}

func (w *WServer) GetLatencyWebsite(args *WWebsiteReq, reply *int) error {
	min, err := GetLatencyHandler(args.URI, args.SamplesPerWorker)
	checkError("GetLatencyHandler: ", err, false)
	fmt.Println("MIN VALUE: ", min)
	*reply = min
	return nil
}

func GetLatencyHandler(uri string, samples int) (int, error) {
	ListOfTimes := []int{}
	samples = 5
	for i := 0; i < samples; i++ {

		start := time.Now()
		resp, err := http.Get(uri)
		checkError("HTTP Get: ", err, false)
		//start := time.Now()
		_, err = ioutil.ReadAll(resp.Body)
		checkError("HTTP Read: ", err, false)

		end := time.Since(start)
		Nend := end.Seconds() * 1000
		ListOfTimes = append(ListOfTimes, int(Nend))
		resp.Body.Close()

	}
	//fmt.Println(ListOfTimes)

	sortedList := sort.IntSlice(ListOfTimes)
	sort.Sort(sortedList)
	min := sortedList[0]
	return min, nil

}

func ParseArguments() (serverIP string, err error) {
	args := os.Args[1:]

	if len(args) == 1 {
		serverIP = args[0]
	} else {
		err = fmt.Errorf("Usage: go run worker.go [server ip:port]")
		return
	}
	return
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
