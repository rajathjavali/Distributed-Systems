package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"strings"
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
	Cmd 	  string
	Key	  string
	Value 	  string
	ClientID  int64
	CommandID int
}


type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database	map[string]string
	clientCmds	map[int64]int
	cmdChan		map[int]chan Op
}

func (kv *RaftKV) InitiateConsensus(newCmd Op) bool {
	index, _, isLeader := kv.rf.Start(newCmd)
	if (isLeader == false) {
		return false
	}
	// wait for consensus to finish
	kv.mu.Lock()
	cmdChan,ok := kv.cmdChan[index]
	if (ok == false) {//make channel
		cmdChan = make(chan Op,1)
		kv.cmdChan[index] = cmdChan
	}
	kv.mu.Unlock()
	select {
	case op := <-cmdChan:
		if(op == newCmd) {
			return true
		} else {
			return false
		}
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here
	var newCmd Op

	newCmd.Cmd = "Get"
	newCmd.Key = args.Key
	newCmd.ClientID = args.ClientID
	newCmd.CommandID = args.CommandID

	status := kv.InitiateConsensus(newCmd)
	if (status) {
		reply.WrongLeader = false

		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.database[args.Key]
		kv.clientCmds[args.ClientID] = args.CommandID
		kv.mu.Unlock()
		return
	}
	reply.WrongLeader = true
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var newCmd Op
	
	newCmd.Cmd = args.Op
	newCmd.Key = args.Key
	newCmd.ClientID = args.ClientID
	newCmd.CommandID = args.CommandID
	newCmd.Value = args.Value

	status := kv.InitiateConsensus(newCmd)
	if (status) {
		reply.WrongLeader = false
		reply.Err = OK
		return
	} 
	reply.WrongLeader = true
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.database = make(map[string]string)
	kv.clientCmds = make(map[int64]int)
	kv.cmdChan = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg,500)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	// wait for commands to appear on apply channel
	go func() {
		for {
			msg := <-kv.applyCh
				op := msg.Command.(Op)
				kv.mu.Lock()
				// checking for old commands
				cmdid,ok := kv.clientCmds[op.ClientID]
			        if(ok == true) {
					if(cmdid < op.CommandID) {//new Cmd
					    if(strings.Compare(op.Cmd,"Put") == 0){
						kv.database[op.Key] = op.Value
					    } else if(strings.Compare(op.Cmd,"Append") == 0) {
						kv.database[op.Key] = kv.database[op.Key] + op.Value
					    }
					    kv.clientCmds[op.ClientID] = op.CommandID
					}
				} else {//new client
					if(strings.Compare(op.Cmd,"Put") == 0){
						kv.database[op.Key] = op.Value
					} else if(strings.Compare(op.Cmd,"Append") == 0) {
						kv.database[op.Key] = kv.database[op.Key] + op.Value
					}
					kv.clientCmds[op.ClientID] = op.CommandID

				}
				cmdChan,ok := kv.cmdChan[msg.Index]
				if (ok == true) {
					select {
						case <-kv.cmdChan[msg.Index]:
						default:
					}
					cmdChan <- op
				} else {
					kv.cmdChan[msg.Index] = make(chan Op, 1)
				}

				kv.mu.Unlock()
		}
	}()

	return kv
}

