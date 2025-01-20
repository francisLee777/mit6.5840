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
	mu sync.Mutex
	// Your definitions here.
	storage map[string]string
	// 如果分成两个 map， 会导致内存测试不过，所以用结构体当做 value
	clientID2LastUniqueIdList map[int64]LastedLog // key 是客户端id，value是最后一次的记录
	// 两个要点， 一是 put 操作不需要返回值，所以不要把 put 记录存储起来，否则内存测试不过。
	// 二是不能像实验提示中的那样采取某些机制删除幂等记录，因为那样无法用一个map来实现，导致无法通过 TestMemPutManyClients 测试
}

type LastedLog struct {
	value string
	reqID int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.storage[args.Key]
	//kv.clientID2LastUniqueIdList[args.ClientID] = LastedLog{
	//	value: reply.Value,
	//	reqID: args.ReqID,
	//}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 这里注意，不能把 put 的结果写到幂等key中，否则内存测试不过，而且上游要求 put 不需要返回值，所以不需要返回
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//kv.clientID2LastUniqueIdList[args.ClientID] = LastedLog{
	//	value: reply.Value,
	//	reqID: args.ReqID,
	//}
	kv.storage[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.clientID2LastUniqueIdList[args.ClientID]; ok && val.reqID == args.ReqID {
		reply.Value = val.value
		return
	}
	kv.clientID2LastUniqueIdList[args.ClientID] = LastedLog{
		value: kv.storage[args.Key],
		reqID: args.ReqID,
	}
	reply.Value = kv.storage[args.Key]
	kv.storage[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.clientID2LastUniqueIdList = make(map[int64]LastedLog)
	return kv
}
