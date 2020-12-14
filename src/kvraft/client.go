package kvraft

import (
	"../labrpc"
	"reflect"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64
	leaderIndex uint32
	leaderLost bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func getErr(reply interface{}) Err {
	return Err(reflect.ValueOf(reply).Elem().FieldByName("Err").String())
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// randomly initialize the leader index
	ck.leaderIndex = uint32(nrand() % int64(len(servers)))
	ck.leaderLost = true
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) getServerIndex() uint32 {
	if ck.leaderLost {
		ck.leaderIndex = (ck.leaderIndex + 1) % uint32(len(ck.servers))
		return ck.leaderIndex
	}
	return ck.leaderIndex
}

func (ck *Clerk) markLeaderLost() {
	ck.leaderLost = true
}

func (ck *Clerk) markLeaderFound(leaderIndex uint32) {
	ck.leaderLost = false
	ck.leaderIndex = leaderIndex
}

func (ck *Clerk) call(svcMeth string, args interface{}, reply interface{}, needRetry func(interface{}) bool) {
	// only exit the loop when a valid result is received from any server
	for {
		leaderIndex := ck.getServerIndex()
		ok := ck.servers[leaderIndex].Call(svcMeth, args, reply)
		if ok && !needRetry(reply) {
			ck.markLeaderFound(leaderIndex)
			return
		}
		ck.markLeaderLost()
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{OpId: nrand(), ClerkId: ck.id, Key: key}
	reply := &GetReply{}
	DPrintf("clerk Get start - %+v", args)
	ck.call("KVServer.Get", args, reply, func(reply interface{}) bool {
		err := reply.(*GetReply).Err
		return !(err == OK || err == ErrNoKey)
	})
	DPrintf("clerk Get reply - %+v: %+v", args, reply)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{OpId: nrand(), ClerkId: ck.id, Key: key, Value: value, Op: op}
	reply := &PutAppendReply{}
	DPrintf("clerk PutAppend start - %+v", args)
	ck.call("KVServer.PutAppend", args, reply, func(reply interface{}) bool {
		err := reply.(*PutAppendReply).Err
		return err != OK
	})
	DPrintf("clerk PutAppend reply - %+v: %+v", reply, args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
