package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Type          string
	PutAppendArgs *PutAppendArgs
	GetArgs       *GetArgs
}

type OpIdentifier struct {
	Index int
	Term int
	OpId int64
}

type OpResult struct {
	Value string
	Err   Err
}

type lastOp struct {
	OpId int64
	Result *OpResult
}

type KVServer struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	dead        int32 // set by Kill()
	resultChans map[OpIdentifier]chan *OpResult
	lastOpMap   map[int64]lastOp // map clerkId to lastOp
	indexMap map[int]OpIdentifier
	termMap map[int]map[OpIdentifier]struct{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
}

func getClerkIdAndOpId(op Op) (int64, int64) {
	switch op.Type {
	case OpTypeGet:
		return op.GetArgs.ClerkId, op.GetArgs.OpId
	case OpTypePutAppend:
		return op.PutAppendArgs.ClerkId, op.PutAppendArgs.OpId
	default:
		panic(fmt.Sprintf("unknown op type: %v", op.Type))
	}
}

func (kv *KVServer) isDuplicateLocked(clerkId, opId int64) bool {
	lastOp, ok := kv.lastOpMap[clerkId]
	return ok && opId == lastOp.OpId
}

func (kv *KVServer) registerOpResultLocked(clerkId, opId int64, result *OpResult) {
	kv.lastOpMap[clerkId] = lastOp{
		OpId:   opId,
		Result: result,
	}
}

func (kv *KVServer) createResultChanLocked(opIdentifier OpIdentifier) chan *OpResult {
	resultChan := make(chan *OpResult, 2)
	kv.indexMap[opIdentifier.Index] = opIdentifier
	kv.resultChans[opIdentifier] = resultChan
	return resultChan
}

func (kv *KVServer) execOp(op Op) (result *OpResult) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return &OpResult{Err: ErrWrongLeader}
	}
	_, opId := getClerkIdAndOpId(op)
	opIdentifier := OpIdentifier{
		Index: index,
		Term:  term,
		OpId:  opId,
	}
	kv.mu.Lock()
	resultChan := kv.createResultChanLocked(opIdentifier)
	kv.mu.Unlock()
	DPrintf("[%v] - execOp - term: %v, index: %v - %v: %+v", kv.me, term, index, opId, op)

	select {
	case result = <- resultChan:
	case <- time.After(time.Second*3):
	}
	if result == nil {
		kv.mu.Lock()
		delete(kv.resultChans, opIdentifier)
		kv.mu.Unlock()
		return &OpResult{Err: ErrWrongLeader}
	}
	return
}

func (kv *KVServer) closeSupercededOp(index, term int) {
	// is it possible that have more than 1 op with the same index?
	// depends on whether raft can delete msg when no new entries
	// are synced.
	opIdentifier, ok := kv.indexMap[index]
	if !ok {
		return
	}
	if opIdentifier.Term == term {
		return
	}
	c, ok := kv.resultChans[opIdentifier]
	if !ok {
		return
	}
	delete(kv.resultChans, opIdentifier)
	c <- &OpResult{Err: ErrWrongLeader}
}

func (kv *KVServer) applier() {
	for {
		DPrintf("[%v] - applier starts waiting for applyCh", kv.me)
		applyMsg := <-kv.applyCh
		op := (applyMsg).Command.(Op)
		index := applyMsg.CommandIndex
		term := applyMsg.CommandTerm
		DPrintf("[%v] - applier got op:%v at index %v, term %v", kv.me, op, index, term)
		clerkId, opId := getClerkIdAndOpId(op)
		// close chan of superceded requests
		opIdentifier := OpIdentifier{
			Index: index,
			Term:  term,
			OpId:  opId,
		}
		kv.mu.Lock()
		//kv.removeFromTermMapLocked(opIdentifier)
		kv.closeSupercededOp(index, term)
		resultChan := kv.resultChans[opIdentifier]
		delete(kv.resultChans, opIdentifier)
		kv.mu.Unlock()
		DPrintf("[%v] - applier applying %+v", kv.me, op)
		switch op.Type {
		case OpTypeGet:
			args := op.GetArgs
			DPrintf("[%v] - applier applying G %v, getting lock", kv.me, opId)
			kv.mu.Lock()
			DPrintf("[%v] - applier applying G %v, got lock", kv.me, opId)
			val, ok := kv.store[args.Key]
			kv.mu.Unlock()
			DPrintf("[%v] - applier applying G %v, unlocked", kv.me, opId)
			if resultChan != nil {
				DPrintf("[%v] - applier applying G %v, putting result into resultChan", kv.me, opId)
				if ok {
					resultChan <- &OpResult{
						Value: val,
						Err:   OK,
					}
				} else {
					resultChan <- &OpResult{
						Err: ErrNoKey,
					}
				}
				DPrintf("[%v] - applier applying G %v, put result into resultChan", kv.me, opId)
			}
		case OpTypePutAppend:
			args := op.PutAppendArgs
			prefix := ""
			key := args.Key
			DPrintf("[%v] - applier applying PA %v, getting lock", kv.me, opId)
			kv.mu.Lock()
			DPrintf("[%v] - applier applying PA %v, got lock", kv.me, opId)
			if !kv.isDuplicateLocked(clerkId, opId) {
				DPrintf("[%v] - applier applying PA %v, not duplicate, apply changes", kv.me, opId)
				if args.Op == OpAppend {
					if val, ok := kv.store[key]; ok {
						prefix = val
					}
				}
				newVal := prefix + args.Value
				kv.store[key] = newVal
				kv.registerOpResultLocked(clerkId, opId, &OpResult{
					Value: newVal,
					Err:   OK,
				})
			}
			kv.mu.Unlock()
			DPrintf("[%v] - applier applying PA %v, unlocked", kv.me, opId)
			if resultChan != nil {
				DPrintf("[%v] - applier applying PA %v, putting result into resultChan", kv.me, opId)
				resultChan <- &OpResult{
					Err:   OK,
				}
				DPrintf("[%v] - applier applying PA %v, put result into resultChan", kv.me, opId)
			}
		default:
			panic(fmt.Sprintf("unknown op type: %v", op.Type))
		}
		DPrintf("[%v] - applied op:%v", kv.me, op)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%v] - server get: %v", kv.me, args)
	res := kv.execOp(Op{
		Type:    OpTypeGet,
		GetArgs: args,
	})
	reply.Value = res.Value
	reply.Err = res.Err
	DPrintf("[%v] - server get: args: %v, reply: %v", kv.me, args, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v] - server put append: args: %v", kv.me, args)
	reply.Err = (kv.execOp(Op{
		Type:          OpTypePutAppend,
		PutAppendArgs: args,
	})).Err
	DPrintf("[%v] - server put append: args: %v, reply: %v", kv.me, args, reply)
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
	kv.resultChans = make(map[OpIdentifier]chan *OpResult)
	kv.store = make(map[string]string)
	kv.lastOpMap = make(map[int64]lastOp)
	kv.indexMap = make(map[int]OpIdentifier)

	go kv.applier()

	// You may need initialization code here.
	return kv
}
