/*
Implements the server in assignment 4

Usage:

go run server.go [worker-incoming ip:port] [client-incoming ip:port]

*/

package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Resource server type.
type MServer int

type SServer int

type WServer int

type InitRes struct {
	WorkerIP string
}

type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

type WWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

type MRes struct {
	Stats map[string]LatencyStats    // map: workerIP -> LatencyStats
	Diff  map[string]map[string]bool // map: [workerIP x workerIP] -> True/False
}

type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

type MWorkersReq struct {
	SamplesPerWorker int // Number of samples, >= 1
}

// Request that client sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP
}

type StartCrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Request that client sends in RPC call to MServer.GetWorkers
type GetWorkersReq struct{}

// Response to MServer.GetWorkers
type GetWorkersRes struct {
	WorkerIPsList []string // List of workerIP string
}

// Request that client sends in RPC call to MServer.Domains
type DomainsReq struct {
	WorkerIP string // IP of worker
}

// Response to MServer.Domains
type DomainsRes struct {
	Domains []string // List of domain string
}

type RedirectReq struct {
	DomainLink string
	NewLink    string
	Dep        int
}

// Request that client sends in RPC call to MServer.Overlap
type OverlapReq struct {
	URL1 string // URL arg to Overlap
	URL2 string // The other URL arg to Overlap
}

// Response to MServer.Overlap
type OverlapRes struct {
	NumPages int
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

type Node struct {
	Parent        string
	Children      []string
	mapOfChildren map[string]Node
}

type SendBack struct {
	ListOut      []string
	ParentNode   string
	ChildrenNode []string
	ChildsNode   map[string]Node
}

var ListOfWIP []string
var ListOfNewIP []string
var tempAddr string
var newStats map[string]int
var WorkerToDomain map[string][]string
var CrawledDepth = map[string][]int{}
var ListOfDepths = []int{}

func main() {
	wIP, cIP, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	done := make(chan int)

	go func() {
		mServer := rpc.NewServer()
		m := new(MServer)
		mServer.Register(m)

		lc, err := net.Listen("tcp", cIP)
		checkError("Listen client: ", err, true)
		for {
			connC, err := lc.Accept()
			checkError("Accept client: ", err, false)
			go mServer.ServeConn(connC)
		}
	}()

	go func() {
		sServer := rpc.NewServer()
		s := new(SServer)
		sServer.Register(s)

		ls, err := net.Listen("tcp", wIP)
		checkError("Listen workers: ", err, true)
		for {
			connS, err := ls.Accept()
			checkError("Accept client: ", err, false)
			tempAddr = connS.RemoteAddr().String()
			// List of workerIP with unique own ports
			fmt.Println("Worker Coming in", tempAddr)
			ListOfWIP = append(ListOfWIP, connS.RemoteAddr().String())
			go sServer.ServeConn(connS)
		}
	}()

	go func() {
		pc, err := net.ListenPacket("udp", wIP)
		checkError("Listen UDP: ", err, true)
		defer pc.Close()

		buffer := make([]byte, 1024)

		for {
			r, addr, err := pc.ReadFrom(buffer)
			checkError("Read UDP: ", err, true)

			if string(buffer[0:r]) == "Ping" {
				//fmt.Println("Received ", string(buffer[0:r]))
				pc.WriteTo([]byte("Pong"), addr)
			}
		}
	}()

	<-done
}

func (s *SServer) GetIPaddress(req bool, res *string) error {
	// TODO: change randomness, what if same already exist
	randnum := rand.Intn(12000-5000) + 5000
	p := ":" + strconv.Itoa(randnum)
	*res = p

	newtempAddr := strings.Split(tempAddr, ":")
	newIP := newtempAddr[0] + p
	// List of workerIP with generated port
	ListOfNewIP = append(ListOfNewIP, newIP)
	return nil
}

func (s *SServer) Redirect(req *RedirectReq, res *bool) error {
	// Check if any worker owns this domain
	for i := 0; i < len(ListOfNewIP); i++ {

		askW, err := rpc.Dial("tcp", ListOfNewIP[i])

		checkError("Check Worker Dial:", err, true)
		args := req.DomainLink
		var reply bool
		error := askW.Call("WServer.GetDomainsForRedirect", args, &reply)
		checkError("GetDomainsForRedirect: ", error, true)
		fmt.Println(ListOfNewIP[i], "REPLY: ", reply)
		if reply {
			fmt.Println("Case 1: A worker owns this domain", ListOfNewIP[i])

			worker, err := rpc.Dial("tcp", ListOfNewIP[i])
			checkError("Worker Dial:", err, true)
			crawlArgs := &StartCrawlReq{req.NewLink, req.Dep}
			var reply bool

			error := worker.Call("WServer.StartCrawl", crawlArgs, &reply)
			checkError("RPC StartCrawl: ", error, false)

			*res = true
			return nil
		}
	}

	fmt.Println("Case 2: Assign to New Worker")
	crawlHandler(req.NewLink, req.Dep)
	//checkError("Redirect Crawl: ", err, false)

	*res = true
	return nil
}

// Returns List of workerIPs
func (m *MServer) GetWorkers(args *GetWorkersReq, reply *GetWorkersRes) error {
	fmt.Println(ListOfNewIP)
	newList := []string{}
	for i := 0; i < len(ListOfNewIP); i++ {
		split1 := strings.Split(ListOfNewIP[i], ":")
		newList = append(newList, split1[0])
	}

	reply.WorkerIPsList = newList
	fmt.Println("Returning List of Worker IP to Client")
	return nil
}

// Crawl the URL
func (m *MServer) Crawl(args *CrawlReq, reply *CrawlRes) error {

	// w := ""
	// fmt.Println(CrawledDepth)
	// if CrawledDepth != nil {
	// 	for k, v := range CrawledDepth {
	// 		if k == args.URL {
	// 			if v >= args.Depth {
	// 				fmt.Println("Already crawled URL at this Depth")
	// 				returnIP := strings.Split(w, ":")
	// 				reply.WorkerIP = returnIP[0]
	// 				fmt.Println("Finish Crawling")
	// 				return nil
	// 			} else {
	// 				fmt.Println("Have not crawled URL at this Depth")
	// 				w = crawlHandler(args.URL, args.Depth)
	// 				CrawledDepth[args.URL] = args.Depth
	// 				returnIP := strings.Split(w, ":")
	// 				reply.WorkerIP = returnIP[0]
	// 				fmt.Println("Finish Crawling")
	// 				return nil

	// 			}
	// 		}
	// 	}
	// }
	fmt.Println(CrawledDepth)
	w := crawlHandler(args.URL, args.Depth)
	ListOfDepths = append(ListOfDepths, args.Depth)
	CrawledDepth[args.URL] = ListOfDepths
	returnIP := strings.Split(w, ":")
	reply.WorkerIP = returnIP[0]
	fmt.Println("Finish Crawling")

	return nil
}

func crawlHandler(url string, depth int) string {
	fmt.Println("Start Crawling")

	// Ask if any worker owns this url's domain first
	removeHttp := strings.TrimPrefix(url, "http://")
	split1 := strings.Split(removeHttp, "/")
	crawlDomain := split1[0]

	for i := 0; i < len(ListOfNewIP); i++ {

		crawlAsk, err := rpc.Dial("tcp", ListOfNewIP[i])

		checkError("crawlAsk Dial:", err, true)
		args := crawlDomain
		var reply bool
		error := crawlAsk.Call("WServer.GetDomainsForRedirect", args, &reply)
		checkError("GetDomainsForRedirect: ", error, true)
		fmt.Println(ListOfNewIP[i], "REPLY: ", reply)
		if reply {
			fmt.Println("Case 1: A worker owns this domain", ListOfNewIP[i])

			if CrawledDepth != nil {
				for k, v := range CrawledDepth {
					if k == url {
						fmt.Println("Crawled this Link before...")
						for j := range v {
							fmt.Println("Comparing", v[j], "to Depth:", depth)
							if v[j] >= depth {
								fmt.Println("Already crawled URL at this Depth")
								return ListOfNewIP[i]
							}

						}

					}
				}
			}

			worker, err := rpc.Dial("tcp", ListOfNewIP[i])
			checkError("Worker Dial:", err, true)
			crawlArgs := &StartCrawlReq{url, depth}
			var reply bool
			error := worker.Call("WServer.StartCrawl", crawlArgs, &reply)
			checkError("RPC StartCrawl: ", error, false)

			return ListOfNewIP[i]
		}
	}

	// Finds the worker to be assigned to the domain.
	ns, err := measureWebsiteHandler(url, 5)
	checkError("measureWebsiteHandler: ", err, false)
	//fmt.Println(ns)

	// minIP is raw IP, might need to add on the port in order to connect to the worker
	minIp := sortLatency(ns)
	fmt.Println(minIp)

	fmt.Println(CrawledDepth)

	if CrawledDepth != nil {
		for k, v := range CrawledDepth {
			if k == url {
				fmt.Println("Crawled this Link before...")
				for j := range v {
					fmt.Println("Comparing", v[j], "to Depth:", depth)
					if v[j] >= depth {
						fmt.Println("Already crawled URL at this Depth")
						return minIp
					}
					// else {
					// 	fmt.Println("Have not crawled URL at this Depth")
					// 	ListOfDepths = append(ListOfDepths, depth)
					// 	CrawledDepth[url] = ListOfDepths
					// 	break
					// }

				}

			}
		}
	}

	// Tell the worker to crawl
	worker, err := rpc.Dial("tcp", minIp)
	checkError("Worker Dial:", err, true)
	crawlArgs := &StartCrawlReq{url, depth}
	var reply bool

	error := worker.Call("WServer.StartCrawl", crawlArgs, &reply)
	checkError("RPC StartCrawl: ", error, false)

	return minIp
}

// Returns list of domains owned by the worker
func (m *MServer) Domains(args *DomainsReq, reply *DomainsRes) error {
	list, err := domainsHandler(args.WorkerIP)
	checkError("domainsHandler: ", err, false)

	reply.Domains = list
	fmt.Println("Returning List of Domains of worker to client")
	return nil
}
func domainsHandler(ipaddr string) ([]string, error) {
	fmt.Println(ListOfNewIP)
	newIPaddr := ipaddr
	for _, i := range ListOfNewIP {
		if strings.HasPrefix(i, ipaddr) {
			newIPaddr = i
		}
	}
	worker, err := rpc.Dial("tcp", newIPaddr)
	checkError("Worker Dial:", err, true)

	var args bool
	var reply []string

	error := worker.Call("WServer.GetDomains", args, &reply)

	return reply, error

}

func measureWebsiteHandler(uri string, samples int) (map[string]int, error) {

	newStats = make(map[string]int)

	removeHttp := strings.TrimPrefix(uri, "http://")
	splitString := strings.Split(removeHttp, "/")
	newURI := "http://" + splitString[0] + "/index.html"

	fmt.Println(newURI)

	for i := 0; i < len(ListOfNewIP); i++ {
		worker, err := rpc.Dial("tcp", ListOfNewIP[i])
		checkError("Worker Dial:", err, true)

		servArgs := &WWebsiteReq{newURI, samples}

		var lat int
		error := worker.Call("WServer.GetLatencyWebsite", servArgs, &lat)
		checkError("RPC GetLatency: ", error, false)

		newStats[ListOfNewIP[i]] = lat
	}

	return newStats, nil
}

func sortLatency(mapOfLat map[string]int) string {
	newMap := map[int][]string{}

	for key, val := range mapOfLat {
		newMap[val] = append(newMap[val], key)
	}

	var listOfMin []int
	for key := range newMap {
		listOfMin = append(listOfMin, key)
	}

	sortedList := sort.IntSlice(listOfMin)
	sort.Sort(sortedList)

	var newA []string
	for _, i := range listOfMin {
		for _, j := range newMap[i] {
			newA = append(newA, j)
		}
	}

	return newA[0]
}

func (m *MServer) Overlap(args *OverlapReq, reply *OverlapRes) error {
	i, err := overlapHandler(args.URL1, args.URL2)
	checkError("overlapHandler: ", err, false)

	reply.NumPages = i
	fmt.Println("Returning overlap pages to client")

	return nil

}

func overlapHandler(url1 string, url2 string) (int, error) {
	var ds []string
	var alreadyGotMap []string
	var urlList []string
	var WList1 []string
	var WList2 []string
	var WorkerDomain []string
	var Map1 Node
	var Map2 Node
	num := 0
	urlList = append(urlList, url1)
	urlList = append(urlList, url2)
	removeHttp := strings.TrimPrefix(url1, "http://")
	split1 := strings.Split(removeHttp, "/")
	urlDomain1 := split1[0]
	ds = append(ds, urlDomain1)
	removeHttp2 := strings.TrimPrefix(url2, "http://")
	split2 := strings.Split(removeHttp2, "/")
	urlDomain2 := split2[0]
	ds = append(ds, urlDomain2)
	fmt.Println("Overlap Domains: ", ds)

	// Check which worker has domain of UR1 and UR2
	// If that worker has it, return the map starting at UR1
	for j := 0; j < len(ds); j++ {
		for i := 0; i < len(ListOfNewIP); i++ {

			askW, err := rpc.Dial("tcp", ListOfNewIP[i])
			checkError("Check Worker Dial:", err, true)
			args := ds[j]
			var reply bool
			error := askW.Call("WServer.GetDomainsForRedirect", args, &reply)
			checkError("GetDomainsForRedirect: ", error, true)

			fmt.Println(ListOfNewIP[i], "REPLY: ", reply)

			if reply {
				fmt.Println("Worker", ListOfNewIP[i], "Owns: ", ds[j])

				// r := false
				//    for _, x := range alreadyGotMap {
				//        	if x != ListOfNewIP[i] {
				//        		continue
				//        	}
				// 	r = true
				//    }
				//    if r {
				//    	fmt.Println("Both domains is owned by the same worker")
				//     return num, nil
				// }

				WorkerDomain = append(WorkerDomain, ListOfNewIP[i])

				askD, err := rpc.Dial("tcp", ListOfNewIP[i])
				checkError("Overlap Dial:", err, true)

				args := WorkerOverlapReq{
					StartURL: urlList[j],
				}

				var reply WorkerOverlapRes
				error := askD.Call("WServer.GetFullMap", args, &reply)
				checkError("GetFullMap: ", error, true)

				alreadyGotMap = append(alreadyGotMap, ListOfNewIP[i])

				if j == 0 {
					WList1 = reply.Outer
					Map1 = Node{reply.Pars, reply.Childs, reply.MChilds}
				} else if j == 1 {
					WList2 = reply.Outer
					Map2 = Node{reply.Pars, reply.Childs, reply.MChilds}
				}

				break
			}
		}
	}

	fmt.Println(WorkerDomain)
	fmt.Println("WList1: ", WList1)
	fmt.Println("WList2: ", WList2)

	if len(WorkerDomain) != 0 {
		// send Lists to the other Worker to compare to their maps, return an int
		sendList, err := rpc.Dial("tcp", WorkerDomain[0])
		checkError("sendList Dial:", err, true)
		args := SendBack{
			ListOut:      WList2,
			ParentNode:   Map1.Parent,
			ChildrenNode: Map1.Children,
			ChildsNode:   Map1.mapOfChildren,
		}
		var res int
		error := sendList.Call("WServer.GoCompare", args, &res)
		checkError("GoCompare: ", error, true)
		fmt.Println("Overlap from URL1: ", res)

		sendList2, err := rpc.Dial("tcp", WorkerDomain[1])
		checkError("sendList Dial:", err, true)
		args2 := SendBack{
			ListOut:      WList1,
			ParentNode:   Map2.Parent,
			ChildrenNode: Map2.Children,
			ChildsNode:   Map2.mapOfChildren,
		}
		var reply int
		error2 := sendList2.Call("WServer.GoCompare", args2, &reply)
		checkError("GoCompare: ", error2, true)
		fmt.Println("Overlap from URL1: ", reply)

		num = reply + res
	}
	// 3 cases, case 1 = locally matched, case 2 = children points, case 3 = no match at all

	return num, nil
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func ParseArguments() (wIP string, cIP string, err error) {
	args := os.Args[1:]

	if len(args) == 2 {

		wIP = args[0]
		cIP = args[1]

	} else {
		err = fmt.Errorf("Usage: go run server.go [worker-incoming ip:port] [client-incoming ip:port]")
		return
	}
	return
}
