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

type Record struct {
	seq   int64
	value string
}

type KVServer struct {
	mu    sync.Mutex
	redis map[string]string
	mem   map[int64]*Record
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.mem[args.ClientId] == nil {
		reply.Value = kv.redis[args.Key]
		return
	}

	if kv.mem[args.ClientId].seq == args.Seq {
		reply.Value = kv.mem[args.ClientId].value
		return
	}

	kv.mem[args.ClientId].seq = args.Seq
	kv.mem[args.ClientId].value = kv.redis[args.Key]
	reply.Value = kv.redis[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.mem[args.ClientId] == nil {
		kv.redis[args.Key] = args.Value
		return
	}

	if kv.mem[args.ClientId].seq == args.Seq {
		return
	}

	kv.mem[args.ClientId].seq = args.Seq
	kv.redis[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.mem[args.ClientId] == nil {
		kv.mem[args.ClientId] = &Record{-1, ""}
	}

	if kv.mem[args.ClientId].seq == args.Seq {
		reply.Value = kv.mem[args.ClientId].value
		return
	}

	kv.mem[args.ClientId].seq = args.Seq
	reply.Value = kv.redis[args.Key]
	kv.mem[args.ClientId].value = reply.Value
	kv.redis[args.Key] = kv.redis[args.Key] + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.redis = make(map[string]string)
	kv.mem = make(map[int64]*Record, 0)

	return kv
}
