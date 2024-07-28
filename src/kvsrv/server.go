package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	kvStore map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, exists := kv.kvStore[key]
	if !exists {
		reply.Exists = false
	} else {
		reply.Value = value
		reply.Exists = true
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	kv.kvStore[key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	oldValue, exists := kv.kvStore[key]
	if !exists {
		oldValue = ""
	}
	kv.kvStore[key] = oldValue + args.Value
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvStore = make(map[string]string)
	return kv
}
