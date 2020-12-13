package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	PutAppendArgs *PutAppendArgs
	GetArgs *GetArgs
}

type OpResult struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	resultChans map[int64]chan *OpResult

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string



}
func getId(op Op) int64 {
	switch op.Type {
	case OpTypeGet:
		return op.GetArgs.Id
	case OpTypePutAppend:
		return op.PutAppendArgs.Id
	default:
		panic(fmt.Sprintf("unknown op type: %v", op.Type))
	}
}
func (kv *KVServer) execOp(op Op) (resultChan chan *OpResult) {
	resultChan = make(chan *OpResult, 1)
	id := getId(op)
	kv.mu.Lock()
	kv.resultChans[id] = resultChan
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		resultChan <- &OpResult{Err: ErrWrongLeader}
		return
	}
	DPrintf("[%v] - execOp - %+v", kv.me, op)
	return
}

func (kv *KVServer) applier() {
	for {
		DPrintf("[%v] - applier starts waiting for applyCh", kv.me)
		op := (<- kv.applyCh).Command.(Op)
		DPrintf("[%v] - applier got op:%v", kv.me, op)
		kv.mu.Lock()
		resultChan := kv.resultChans[getId(op)]
		delete(kv.resultChans, getId(op))
		kv.mu.Unlock()
		DPrintf("[%v] - applier applying %+v", kv.me, op)
		switch op.Type {
		case OpTypeGet:
			args := op.GetArgs
			kv.mu.Lock()
			val, ok := kv.store[args.Key]
			kv.mu.Unlock()
			if resultChan != nil {
				if ok {
					resultChan <- &OpResult{
						Value: val,
						Err:   OK,
					}
				} else {
					resultChan <- &OpResult{
						Err:   ErrNoKey,
					}
				}
			}
		case OpTypePutAppend:
			args := op.PutAppendArgs
			prefix := ""
			key := args.Key
			kv.mu.Lock()
			if args.Op == OpAppend {
				if val, ok := kv.store[key]; ok {
					prefix = val
				}
			}
			newVal := prefix + args.Value
			kv.store[key] = newVal
			kv.mu.Unlock()
			if resultChan != nil {
				resultChan <- &OpResult{
					Value: newVal,
					Err:   OK,
				}
			}
		default:
			panic(fmt.Sprintf("unknown op type: %v", op.Type))
		}
		DPrintf("[%v] - applied op:%v", kv.me, op)
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	res := <- kv.execOp(Op{
		Type:          OpTypeGet,
		GetArgs:       args,
	})
	reply.Value = res.Value
	reply.Err = res.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v] - server put append: args: %v", kv.me, args)
	reply.Err = (<- kv.execOp(Op{
		Type:          OpTypePutAppend,
		PutAppendArgs: args,
	})).Err
	DPrintf("[%v] - server put append: reply: %v", kv.me, reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultChans = make(map[int64]chan *OpResult)
	kv.store = make(map[string]string)

	go kv.applier()

	// You may need initialization code here.
	return kv
}
