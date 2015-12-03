package main

import "fmt"
import "github.com/garyburd/redigo/redis"
import "github.com/emirpasic/gods/maps/treemap"
import "strconv"
import "hash/crc64"
import "net/http"
import "encoding/json"
import "os/exec"
import "github.com/gorilla/mux"
import "strings"
import "time"
import "os"
import "log"
import "bufio"
import "bytes"
import "io/ioutil"

var redisNodesMap map[Node]redis.Conn = make(map[Node]redis.Conn)
var nodeHashMap map[uint64]Node = make(map[uint64]Node)
var circle *treemap.Map = treemap.NewWith(UInt64Comparator)
var myport int = 8090

type Node struct {
    Port int `json:"port"`
}

type GetKeyValueRes struct {
    Key   int    `json:"key"`
    Value string `json:"value"`
}

type NodeRes struct {
    Port   int    `json:"port"`
    Count int64 `json:"count"`
}

type NodeReplica struct {
    Port   int    `json:"port"`
    ReplicaCount int `json:"replicacount"`
}

type KeyValue struct {
    Key   string    `json:"key"`
    Value string `json:"value"`
}

type AllNodeInfo struct {
    Allnodes []NodeRes `json:"allnodes"`
}

type GetAllKeysRes []KeyValue

type PutKeyValueResponse struct {
    Message string `json:"message"`
}

func UInt64Comparator(a, b interface{}) int {
    aInt := a.(uint64)
    bInt := b.(uint64)
    switch {
    case aInt > bInt:
        return 1
    case aInt < bInt:
        return -1
    default:
        return 0
    }
}

//add node to circle treemap
func AddToCircle(node Node) uint64 {
    //Generate a string identifier for node and add to circle
    nodeStr := "localhost:" + strconv.Itoa(node.Port)
    hash := Hashcode(nodeStr)
    circle.Put(hash, nodeStr)
    fmt.Println("Added Node", nodeStr, "Hashcode", hash)
    return hash
}

//remove node from circle treemap
func RemoveFromCircle(node Node) {
    //Generate a string identifier for node and add to circle
    nodeStr := "localhost:" + strconv.Itoa(node.Port)
    circle.Remove(Hashcode(nodeStr))
    fmt.Println("Removed Node", nodeStr, "Hashcode", Hashcode(nodeStr))
}

//hash function
func Hashcode(data string) uint64 {
    crcTable := crc64.MakeTable(crc64.ECMA)
    return crc64.Checksum([]byte(data), crcTable)
}

// function to get hash value of node which will store the key-value pair
func GetNodeHashForKey(key string) uint64 {

    keyHash := Hashcode(key)
    keys := circle.Keys()

    return keys[getNodeIndex(keys, keyHash, 0)].(uint64)
}

func getNodeIndex(keys []interface{}, keyHash uint64, index int) int {

    if index < circle.Size() && keyHash > keys[index].(uint64) {
        return getNodeIndex(keys, keyHash, index+1)
    } else if index < circle.Size() && keyHash < keys[index].(uint64) {
        return index
    } else if index == circle.Size() {
        return 0
    } else {
        return 0
    }
}

func main() {

    //initially create four nodes and connect them to four redis servers
    node1 := Node{7379}
    node2 := Node{8451}
    node3 := Node{9233}

    cmd1 := exec.Command("redis-server", "--port " + "7379")
    commandOutput(cmd1)
    cmd1.Start()
    time.Sleep(100 * time.Millisecond)

    cmd2 := exec.Command("redis-server", "--port " + "8451")
    commandOutput(cmd2)
    cmd2.Start()
    time.Sleep(100 * time.Millisecond)

    cmd3 := exec.Command("redis-server", "--port " + "9233")
    commandOutput(cmd3)
    cmd3.Start()
    time.Sleep(100 * time.Millisecond)

    //create three redis connections
    conn1, _ := redis.Dial("tcp", "127.0.0.1:"+strconv.Itoa(node1.Port))
    conn2, _ := redis.Dial("tcp", "127.0.0.1:"+strconv.Itoa(node2.Port))
    conn3, _ := redis.Dial("tcp", "127.0.0.1:"+strconv.Itoa(node3.Port))

    conn1.Do("FLUSHDB")
    conn2.Do("FLUSHDB")
    conn3.Do("FLUSHDB")

    //map nodes and redis connections
    redisNodesMap[node1] = conn1
    redisNodesMap[node2] = conn2
    redisNodesMap[node3] = conn3

    //add nodes to circle treemap in consistenthashing.go and nodeHashMap
    nodeHashMap[AddToCircle(node1)] = node1
    nodeHashMap[AddToCircle(node2)] = node2
    nodeHashMap[AddToCircle(node3)] = node3

    //store sample key-value pairs

    input1 := "name"
    input2 := "age"
    input3 := "address"
    input4 := "city"
    input5 := "state"
    input6 := "country"

    input1Value := "Shikher Pandey"
    input2Value := "26"
    input3Value := "201 S 4th Street"
    input4Value := "San Jose"
    input5Value := "CA"
    input6Value := "US"

    storeKeyValue(input1, input1Value)
    storeKeyValue(input2, input2Value)
    storeKeyValue(input3, input3Value)
    storeKeyValue(input4, input4Value)
    storeKeyValue(input5, input5Value)
    storeKeyValue(input6, input6Value)

    //code for storing one million keys

    fmt.Println("One lacs Start")

    for i:= 0; i < 100000; i++ {
        key := "key " + strconv.Itoa(i)
        value:= "Value of key " + strconv.Itoa(i) + " is value " + strconv.Itoa(i)
        storeKeyValue(key,value)
    }

    fmt.Println("One lacs End")

    rtr := mux.NewRouter()
    rtr.HandleFunc("/keys/{key_id}", getKeyValueReq).Methods("GET")
    rtr.HandleFunc("/nodes/{port}", postNode).Methods("POST")
    rtr.HandleFunc("/nodes", keyCount).Methods("GET")
    rtr.HandleFunc("/nodes/{port}", deleteNode).Methods("DELETE")
    rtr.HandleFunc("/keys/{key_id}/{value}", storeKeyValueReq).Methods("PUT")
    rtr.HandleFunc("/notify/{port}",postNotifyClient).Methods("POST")
    rtr.HandleFunc("/notify/{port}",deleteNotifyClient).Methods("DELETE")
    http.Handle("/", &MyServer{rtr})
    http.ListenAndServe(":" + strconv.Itoa(myport), nil);
}

type MyServer struct {
    r *mux.Router
}

func (s *MyServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
    if origin := req.Header.Get("Origin"); origin != "" {
        rw.Header().Set("Access-Control-Allow-Origin", origin)
        rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
        rw.Header().Set("Access-Control-Allow-Headers",
            "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
    }
    // Stop here if its Preflighted OPTIONS request
    if req.Method == "OPTIONS" {
        return
    }
    // Lets Gorilla work
    s.r.ServeHTTP(rw, req)
}

func storeKeyValue(key string, value string) KeyValue{
    node := nodeHashMap[GetNodeHashForKey(key)]
    keyvalue := KeyValue{}

    //loop through redis node map and find the appropriate connection

    for index, _ := range redisNodesMap {
        if index.Port == node.Port {
            conn := redisNodesMap[index]
            //set the key value to redis node
            conn.Do("SET", key, value)
            keyvalue.Key = key
            keyvalue.Value = value
            break
        }
    }
    return keyvalue
}

func getKeyValue(key string) KeyValue{
    node := nodeHashMap[GetNodeHashForKey(key)]
    keyvalue := KeyValue{}

    //loop through redis node map and find the appropriate connection

    for index, _ := range redisNodesMap {
        if index.Port == node.Port {
            conn := redisNodesMap[index]
            //set the key value to redis node
            value, _ := redis.String(conn.Do("GET", key))
            fmt.Println(value)
            keyvalue.Key = key
            keyvalue.Value = value
            break
        }
    }
    return keyvalue
}

func addNode(node Node) NodeRes{
    /*Logic to redistribute keys
      1. Find hash value for new node
      2. Check in which old node it is going
      3. Transfer those keys from old node to new node whose hash value is less than hash of new new node
      4. Update redisNodesMap,nodeHashMap
    */

    nodeStr := "localhost:" + strconv.Itoa(node.Port)
    cmd := exec.Command("redis-server", "--port " + strconv.Itoa(node.Port))
    commandOutput(cmd)
    cmd.Start()
    time.Sleep(40 * time.Millisecond)

    conn, _ := redis.Dial("tcp", "127.0.0.1:"+strconv.Itoa(node.Port))
    conn.Do("FLUSHDB")

    oldNode := nodeHashMap[GetNodeHashForKey(nodeStr)]
    noderes := NodeRes{}

    for index, _ := range redisNodesMap {
        if index.Port == oldNode.Port {

            //add node to redisNodesMap, nodeHashMap
            redisNodesMap[node] = conn
            hashOfNode := AddToCircle(node)
            nodeHashMap[hashOfNode] = node
            noderes.Port = node.Port

            connOld := redisNodesMap[index]
            nodeOldStr := "localhost:" + strconv.Itoa(oldNode.Port)
            nodeOldHash := Hashcode(nodeOldStr)

            keys, err := redis.Strings(connOld.Do("KEYS", "*"))
            if err != nil {
                panic(err.Error())
            }

            for _, key := range keys {
                value, _ := redis.String(connOld.Do("GET", key))

                //check for hash value for each and compare it with new node

                if GetNodeHashForKey(key) != nodeOldHash {
                     storeKeyValue(key,value)
                     connOld.Do("DEL", key)
                 }
            }

            break
        }
    }
    keysCount, _ := conn.Do("DBSIZE")
    fmt.Println(keysCount)
    noderes.Count = keysCount.(int64)
    notifyAddNode(node.Port)
    return noderes
}

func removeNode(node Node) NodeRes{

    //Logic to redistribute keys

    var allKeyValuePairs map[string]string = make(map[string]string)
    noderes := NodeRes{}

    //get all key - value pairs from node

    for index, _ := range redisNodesMap {
        if index.Port == node.Port {

            conn := redisNodesMap[index]

            keys, err := redis.Strings(conn.Do("KEYS", "*"))
            if err != nil {
                panic(err.Error())
            }

            for _, key := range keys {
                value, _ := redis.String(conn.Do("GET", key))
                allKeyValuePairs[key] = value
            }
            keysCount, _ := conn.Do("DBSIZE")
            noderes.Count = keysCount.(int64)
            conn.Do("FLUSHDB")
            break
        }
    }

    //remove node from redisNodesMap, nodeHashMap

    nodeStr := "localhost:" + strconv.Itoa(node.Port)
    hash := Hashcode(nodeStr)
    noderes.Port = node.Port

    for index, _ := range redisNodesMap {
        if index.Port == node.Port {
            delete(redisNodesMap, index)
            fmt.Println("length of redis map" + strconv.Itoa(len(redisNodesMap)))
            break
        }
    }

    for index2, _ := range nodeHashMap {
        if index2 == hash {
            delete(nodeHashMap, index2)
            fmt.Println("length of node map" + strconv.Itoa(len(nodeHashMap)))
            break
        }
    }
    RemoveFromCircle(node)
    //store all keys of the map
    for index3, value3 := range allKeyValuePairs {
        storeKeyValue(index3, value3)
    }

    cmd := exec.Command("redis-cli", "-h 127.0.0.1 " + "-p " + strconv.Itoa(node.Port) + " shutdown")
    commandOutput(cmd)
    cmd.Start()
    time.Sleep(40 * time.Millisecond)

    notifyDeleteNode(node.Port)
    return noderes
}

func storeKeyValueReq(rw http.ResponseWriter, req *http.Request) {
    p:=mux.Vars(req)

    key := p["key_id"]
    value := p["value"]
    pkvr := storeKeyValue(key, value)

    resJson, _ := json.Marshal(pkvr)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resJson)

}

func getKeyValueReq(rw http.ResponseWriter, req *http.Request) {
    p:=mux.Vars(req)
    key := p["key_id"]
    gkvr := getKeyValue(key)

    resJson, _ := json.Marshal(gkvr)
    fmt.Println("Get Key Value Response ", gkvr)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resJson)

}

func postNode(rw http.ResponseWriter, req *http.Request) {
    p:=mux.Vars(req)
    portString := p["port"]
    port, _ := strconv.Atoi(portString)
    node := Node{port}

    noderes := addNode(node)
    resJson, _ := json.Marshal(noderes)

    fmt.Println("Added Node ", noderes)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(201)
    fmt.Fprintf(rw, "%s", resJson)
}

func deleteNode(rw http.ResponseWriter, req *http.Request) {
    p:=mux.Vars(req)
    portString := p["port"]
    port, _ := strconv.Atoi(portString)
    node := Node{port}

    noderes := removeNode(node)
    resJson, _ := json.Marshal(noderes)

    fmt.Println("Removed Node ", noderes)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resJson)
}

func keyCount(rw http.ResponseWriter, req *http.Request) {
    //p:=mux.Vars(req)
    var allnodes []NodeRes
    allnodeinfo := AllNodeInfo{}
    for node, conn := range redisNodesMap {
        nodeinfo := NodeRes{}
        nodeinfo.Port = node.Port
        keysCount, _ := conn.Do("DBSIZE")
        if(keysCount!=nil) {
            nodeinfo.Count = keysCount.(int64)            
        } else {
            nodeinfo.Count = 0
        }
        allnodes = append(allnodes, nodeinfo)
    }
    allnodeinfo.Allnodes = allnodes
    resJson, _ := json.Marshal(allnodeinfo)

    fmt.Println("All Nodes ", allnodeinfo)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(200)
    fmt.Fprintf(rw, "%s", resJson)
}

func notifyAddNode(nodePort int) {

    pwd, _ := os.Getwd()
    file, err := os.Open(pwd + "/client2.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    // var string []clientInfo
    for scanner.Scan() {
    
        s := strings.Split(scanner.Text(), " ")
        portInfo := strings.Split(s[0], ":")
        portInt, _ := strconv.Atoi(portInfo[1])
        if(portInt != myport) {
            url := "http://localhost:" + portInfo[1] + "/" + "notify/" + strconv.Itoa(nodePort)
            req, err := http.NewRequest("POST", url, bytes.NewBuffer(nil))
            req.Header.Set("Content-Type", "application/json")
            client := &http.Client{}
            resp, err := client.Do(req)
            if err != nil {
                panic(err)
            }
            defer resp.Body.Close()

            body, _ := ioutil.ReadAll(resp.Body)
            var data Node

            err = json.Unmarshal(body, &data)

            if err != nil {
                panic(err)
            }            
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

func notifyDeleteNode(nodePort int) {

    pwd, _ := os.Getwd()
    file, err := os.Open(pwd + "/client1.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

    for scanner.Scan() {
    
        s := strings.Split(scanner.Text(), " ")
        portInfo := strings.Split(s[0], ":")
        portInt, _ := strconv.Atoi(portInfo[1])
        if(portInt != myport) {
            url := "http://localhost:" + portInfo[1] + "/" + "notify/" + strconv.Itoa(nodePort)
            req, err := http.NewRequest("DELETE", url, bytes.NewBuffer(nil))
            req.Header.Set("Content-Type", "application/json")
            client := &http.Client{}
            resp, err := client.Do(req)
            if err != nil {
                panic(err)
            }
            defer resp.Body.Close()
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

//notify all other clients about creation of node
func postNotifyClient(rw http.ResponseWriter, req *http.Request) {

    p:=mux.Vars(req)
    port := p["port"]
    node := Node {}
    portInt, _ := strconv.Atoi(port)
    node.Port = portInt
    conn, _ := redis.Dial("tcp", "127.0.0.1:"+strconv.Itoa(node.Port))

    redisNodesMap[node] = conn
    hashOfNode := AddToCircle(node)
    nodeHashMap[hashOfNode] = node

    resJson, _ := json.Marshal(node)

    rw.Header().Set("Content-Type", "application/json")

    rw.WriteHeader(201)
    fmt.Fprintf(rw, "%s", resJson)
}

//notify all other clients about deletion of node
func deleteNotifyClient(rw http.ResponseWriter, req *http.Request) {
    p:=mux.Vars(req)

    port := p["port"]
    node := Node {}
    portInt, _ := strconv.Atoi(port)
    node.Port = portInt
    nodeStr := "localhost:" + strconv.Itoa(node.Port)
    hash := Hashcode(nodeStr)

    for index, _ := range redisNodesMap {
        if index.Port == node.Port {
            delete(redisNodesMap, index)
            break
        }
    }

    for index2, _ := range nodeHashMap {
        if index2 == hash {
            delete(nodeHashMap, index2)
            break
        }
    }

    rw.Header().Set("Content-Type", "application/json")

    rw.WriteHeader(200)
}

func commandOutput(cmd *exec.Cmd) {
  fmt.Printf("==> Running: %s\n", strings.Join(cmd.Args, " "))
}
