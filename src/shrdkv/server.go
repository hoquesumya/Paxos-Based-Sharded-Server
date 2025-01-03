package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"reflect"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0
const (
	PutTask      = "PUT"
	GetTask      = "GET"
	PutHashTask  = "PUTHASH"
	GiveShard    = "GIVESHARD"
	RECEIVESHARD = "RECEIVESHARD"
	UPDATECONFIG = "UpdateConfig"
	UNAVAILABLE  = "Unav"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Key             string
	Value           string
	OpsType         string
	ClientId        int64
	ReconfigID      int64
	Shard           int
	PrevConfig      shardmaster.Config
	NewConfig       shardmaster.Config
	Nconfig         int
	ME              int
	TrasferredShard map[int]ShardsDB
}

type ShardKV struct {
	mu         sync.Mutex
	view       sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid            int64              // my replica group ID
	config         shardmaster.Config //my own view
	shardstate     map[int]ShardsDB
	availableShard map[int][10]bool //cofig --> shard
	configChange   bool
	extraInfo      map[int64]string

	// Your definitions here.
}

//Paxos functions

func (kv *ShardKV) waitUntilDone(n int) Op {
	t := 10 * time.Millisecond
	time.Sleep(t)
	for {
		dc, vals := kv.px.Status(n)
		if Debug > 0 {
			fmt.Printf("[GID %d][Server %d],my write op for seq# %d is %t, %v\n", kv.gid, kv.me, n, dc, vals)
		}
		if dc {
			op, _ := vals.(Op)
			return op
		}
		time.Sleep(t)

		if t < 10*time.Second {
			t *= 2
		}

	}
}

func (kv *ShardKV) apply_all_op(n_next int) {
	n_prev := kv.px.PeerDone[kv.me] + 1 // or possibly track this as kv.lastAppliedSeq if available

	op := Op{}
	for i := n_prev; i <= n_next; i++ {
		kv.px.Start(i, op)
		recvd_val := kv.waitUntilDone(i)
		kv.handlePreiousSEQ(recvd_val)
		if Debug > 0 {
			//fmt.Printf("...kvServer..[Server %d],checking decided value for %d\n", kv.me, i)
		}
	}
}
func (kv *ShardKV) catchup() int {
	n := kv.px.Max()
	kv.apply_all_op(n)
	return n + 1
}

func (kv *ShardKV) startPaxos(op Op, next_max int) {
	for {
		kv.px.Start(next_max, op)
		recvd_val := kv.waitUntilDone(next_max)

		if reflect.DeepEqual(recvd_val, op) {
			break
		}
		kv.handlePreiousSEQ(recvd_val)
		if op.OpsType == GetTask || op.OpsType == PutTask || op.OpsType == PutHashTask {
			//config has been changed ; cant' process client request
			if kv.config.Num != op.Nconfig || !kv.availableShard[op.Nconfig][op.Shard] {
				return
			}
		}
		next_max++
	}
	if Debug > 0 {
		fmt.Printf("...kvServer...Server[%d]<<<>>>\n", kv.me)
	}
	kv.px.Done(next_max - 1)
}

func (kv *ShardKV) handlePut(op Op) (Err, string) {
	v, ok := kv.shardstate[op.Shard].ClientState[op.ClientId]
	if ok {
		return OK, v
	}
	res := ""
	_, ok = kv.shardstate[op.Shard]
	if !ok {
		DPrintf("[GID %d][Server %d], the shard does not exist", kv.gid, kv.me)
		kv.shardstate[op.Shard] = ShardsDB{
			Database:    make(map[string]string),
			ClientState: make(map[int64]string),
		}
	} else {
		DPrintf("[GID %d][Server %d], Current keys in shard %d: %v", kv.gid, kv.me, op.Shard, kv.shardstate[op.Shard].Database)

		val, is_exist := kv.shardstate[op.Shard].Database[op.Key]
		if is_exist {
			res = val
			//DPrintf("[GID %d][Server %d], the key exisit already %s\n", kv.gid, kv.me, res)
		}
		DPrintf("[GID %d][Server %d], the key is %s,  %s\n", kv.gid, kv.me, op.Key, val)
	}

	if op.OpsType == PutTask {
		kv.shardstate[op.Shard].Database[op.Key] = op.Value
	} else {
		DPrintf("[GID %d][Server %d], the value is %s\n", kv.gid, kv.me, res)
		kv.shardstate[op.Shard].Database[op.Key] = strconv.Itoa(int(hash(res + op.Value)))
	}
	kv.shardstate[op.Shard].ClientState[op.ClientId] = res
	DPrintf("[GID %d][Server %d], the current database for the shard %d is %v\n", kv.gid, kv.me, op.Shard, kv.shardstate[op.Shard])
	return OK, res

}
func (kv *ShardKV) handleGet(op Op) (Err, string) {
	//p, ok := kv.shardstate[op.Shard].ClientState[op.ClientId]
	//if ok {
	//return OK, p
	//}
	val := ""
	_, ok := kv.shardstate[op.Shard]
	if !ok {
		return ErrNoKey, ""
	} else {
		v, ok := kv.shardstate[op.Shard].Database[op.Key]
		if !ok {
			return ErrNoKey, ""
		} else {
			val = v
		}
	}
	//kv.shardstate[op.Shard].ClientState[op.ClientId] = val
	DPrintf("[GID %d][Server %d], the current database for the shard %d is %v\n", kv.gid, kv.me, op.Shard, kv.shardstate[op.Shard])
	return OK, val
}
func (kv *ShardKV) handleUpdate(op Op) {
	if kv.config.Num < op.NewConfig.Num {
		kv.config = op.NewConfig
		//then make the prevconfig shards false that is going to be tranferred to the new config
		temp := kv.availableShard[op.PrevConfig.Num]
		for shard, gid := range op.PrevConfig.Shards {
			newGid := op.NewConfig.Shards[shard]
			if gid == kv.gid && newGid != kv.gid {
				temp[shard] = false
			}
		}
		kv.availableShard[op.PrevConfig.Num] = temp
		//now for the new config make the shard positive which has not been transferred
		temp = kv.availableShard[op.NewConfig.Num]
		for shard, gid := range op.PrevConfig.Shards {
			if (gid == kv.gid || gid == 0) && op.NewConfig.Shards[shard] == kv.gid {
				temp[shard] = true
			}
		}
		kv.availableShard[op.NewConfig.Num] = temp
	}
}
func (kv *ShardKV) handleReceiveShards(op Op) {
	DPrintf("[GID %d][Server %d] now applying the receive shard ops", kv.gid, kv.me)
	if _, exist := kv.extraInfo[op.ReconfigID]; exist {
		return
	}
	done := false
	//situation when paxos replicas got the shard for the same config
	for shard, _ := range op.TrasferredShard {
		if kv.availableShard[op.Nconfig][shard] {
			done = true
		}
	}
	if done {
		return
	}
	temp := kv.availableShard[op.Nconfig]
	for shard, info := range op.TrasferredShard {
		DPrintf("[GID %d][Server %d] the shard  %d and info that are transferred %v", kv.gid, kv.me, shard, info)
		temp[shard] = true
		/*kv.shardstate[shard] = ShardsDB{
			Database:    make(map[string]string),
			ClientState: make(map[int64]string),
		}
		for key, val := range info.Database {
			kv.shardstate[shard].Database[key] = val
		}
		for key, val := range info.ClientState {
			kv.shardstate[shard].ClientState[key] = val
		}*/
		if _, exists := kv.shardstate[shard]; !exists {
			kv.shardstate[shard] = ShardsDB{
				Database:    make(map[string]string),
				ClientState: make(map[int64]string),
			}
		}
		// Copy the transferred Database
		for key, value := range info.Database {
			kv.shardstate[shard].Database[key] = value
		}

		// Copy the transferred ClientState
		for clientID, clientState := range info.ClientState {
			kv.shardstate[shard].ClientState[clientID] = clientState
		}

	}
	kv.availableShard[op.Nconfig] = temp
	kv.extraInfo[op.ReconfigID] = op.OpsType
	for key, val := range kv.shardstate {
		DPrintf("[GID %d][Server %d] Prijtng the DB from receive shard %d--> %v\n", kv.gid, kv.me, key, val)
	}
}
func (kv *ShardKV) handlePreiousSEQ(op Op) {
	if op.OpsType == PutTask || op.OpsType == PutHashTask {
		DPrintf("[GID %d][SERVER %d] applying the put ops\n", kv.gid, kv.me)
		kv.handlePut(op)
	} else if op.OpsType == GetTask {
		DPrintf("[GID %d][SERVER %d] applying the get ops\n", kv.gid, kv.me)
		kv.handleGet(op)
	} else if op.OpsType == UPDATECONFIG {
		kv.handleUpdate(op)
	} else if op.OpsType == RECEIVESHARD {
		kv.handleReceiveShards(op)

	}

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*if kv.configChange {
		reply.Err = ErrNotReady
		DPrintf("[GID %d][Server %d], cant accpet the client get request because of in the midde of tranferring\n", kv.gid, kv.me)
		return nil
	}
	*/
	if Debug > 0 {
		fmt.Printf("[GID %d][Server %d] got the request for key %s\n", kv.gid, kv.me, args.Key)
	}
	next_max := kv.catchup()
	if kv.configChange {
		reply.Err = ErrNotReady
		DPrintf("[GID %d][Server %d], cant accpet the client get request because of in the midde of tranferring\n", kv.gid, kv.me)
		return nil
	}

	if args.Nconfig != kv.config.Num || !kv.availableShard[args.Nconfig][args.Shard] {
		DPrintf("[GID %d][Server %d], cant accpet the client get request because of either wrong config or not received shard yet\n", kv.gid, kv.me)
		reply.Err = ErrNotReady
		return nil
	}
	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("[GID %d][Server %d], cant accpet the client get request I don't have the shard  %d that client wants yet %v\n", kv.gid, kv.me, args.Shard, kv.config.Shards)
		return nil
	}

	val := Op{}
	val.Key = args.Key
	val.Value = ""
	val.OpsType = GetTask
	val.ClientId = args.CientID
	val.Shard = args.Shard
	val.Nconfig = args.Nconfig

	kv.startPaxos(val, next_max)
	reply.Err, reply.Value = kv.handleGet(val)
	//first agree to the value
	return nil

}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	/*if kv.configChange {
		reply.Err = ErrNotReady
		DPrintf("[GID %d][Server %d], cant accpet the client put request because of in the midde of tranferring\n", kv.gid, kv.me)
		return nil
	}*/

	if Debug > 0 {
		fmt.Printf("[GID %d][Server %d] got the put req for key %s , val %s for shard %d\n", kv.gid, kv.me, args.Key, args.Value, args.Shard)
	}
	next_max := kv.catchup()
	if kv.configChange {
		reply.Err = ErrNotReady
		DPrintf("[GID %d][Server %d], cant accpet the client put request because of in the midde of tranferring\n", kv.gid, kv.me)
		return nil
	}

	if args.Nconfig != kv.config.Num || !kv.availableShard[args.Nconfig][args.Shard] {
		DPrintf("[GID %d][Server %d], cant accpet the client put request because of either wrong config or not received shard yet\n", kv.gid, kv.me)
		reply.Err = ErrNotReady
		return nil
	}
	if kv.config.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("[GID %d][Server %d], cant accpet the client get request I don't have the shard  %d that client wants yet %v\n", kv.gid, kv.me, args.Shard, kv.config.Shards)
		return nil
	}
	val := Op{}
	val.Key = args.Key

	if args.DoHash {
		val.OpsType = PutHashTask
	} else {
		val.OpsType = PutTask
	}
	val.Value = args.Value
	val.ClientId = args.CientID
	val.Shard = args.Shard
	val.Nconfig = args.Nconfig

	kv.startPaxos(val, next_max)

	reply.Err, reply.PreviousValue = kv.handlePut(val)

	//first agree to the value

	return nil

}
func (kv *ShardKV) ReceiveShard(args *TransferShardsArgs, reply *TransferShardsReply) error {
	fmt.Printf("...kvServer...[GID %d][Server %d]got the shard from gid %d\n", kv.gid, kv.me, args.Gid)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	n := kv.catchup()

	if args.Nconfig > kv.config.Num {
		DPrintf("[GID %d][Server %d] cant' accept the shard, my config %d, the recieved config %d \n", kv.gid, kv.me, kv.config.Num, args.Nconfig)
		reply.Err = ErrNotReady
		return nil
	}
	for shard, _ := range args.ShardInfo {
		if ok := kv.availableShard[args.Nconfig][shard]; ok {
			DPrintf("Found duplicate %v:%v", kv.gid, kv.me)
			reply.Err = ErrDuplicate
			return nil
		}
	}
	recieveOp := Op{}
	recieveOp.ReconfigID = nrand()
	recieveOp.OpsType = RECEIVESHARD
	recieveOp.Nconfig = args.Nconfig
	recieveOp.TrasferredShard = args.ShardInfo
	DPrintf("[GID %d][SERVER %d] the transferred shard %v and args shard %v", kv.gid, kv.me, recieveOp.TrasferredShard, args.ShardInfo)

	kv.startPaxos(recieveOp, n)
	kv.handleReceiveShards(recieveOp)
	for key, val := range kv.shardstate {
		DPrintf("[GID %d][Server %d] Prijtng  again to check the update the DB from receive shard %d--> %v\n", kv.gid, kv.me, key, val)
	}

	return nil

}
func (kv *ShardKV) giveShard(prev shardmaster.Config, newCon shardmaster.Config) {
	//this funtion is responsible for handing out shards if there is
	des := make(map[int64][]int) // gid --> []Shards
	//getting the shards and its destination
	for shard, prevgid := range prev.Shards {
		newGid := newCon.Shards[shard]
		if kv.gid == prevgid && newGid != kv.gid {
			des[newGid] = append(des[newGid], shard)
		}
	}
	fmt.Printf("...kvServer...[GID %d][Server %d]prevConfig and Newconfig is %v, Num %d, %v Num %d\n", kv.gid, kv.me, prev.Shards, newCon.Shards, prev.Num, newCon.Num)
	if Debug > 0 {
		fmt.Printf("...kvServer...[GID %d][Server %d]priting the key-val from giveshard opn\n", kv.gid, kv.me)
		for shard, keyval := range kv.shardstate {
			fmt.Printf("...kvServer..[GID %d][Server %d]|shrd %d|---> |keyval %v|\n", kv.gid, kv.me, shard, keyval)
		}
	}

	if Debug > 0 {
		for desGid, shards := range des {
			fmt.Printf("[GID %d][Server %d] the  transferrable gid and shards are %d, %v\n", kv.gid, kv.me, desGid, shards)
		}
	}
	n := kv.catchup()
	kv.startPaxos(Op{}, n)

	for desGid, shards := range des {
		args := &TransferShardsArgs{}
		args.ShardInfo = make(map[int]ShardsDB)
		args.Nconfig = prev.Num + 1

		//now insert the shard inside the database

		/*for _, shard := range shards {
			args.ShardInfo[shard] = ShardsDB{
				Database:    make(map[string]string),
				ClientState: make(map[int64]string),
			}
			args.Shard = shard

			for key, val := range kv.shardstate[shard].Database {
				args.ShardInfo[shard].Database[key] = val
			}
			for key, val := range kv.shardstate[shard].ClientState {
				args.ShardInfo[shard].ClientState[key] = val
			}
		}*/

		done := false
		for !done {
			n = kv.catchup()
			kv.startPaxos(Op{}, n)
			for _, shard := range shards {
				args.ShardInfo[shard] = ShardsDB{
					Database:    make(map[string]string),
					ClientState: make(map[int64]string),
				}
				args.Shard = shard

				for key, val := range kv.shardstate[shard].Database {
					args.ShardInfo[shard].Database[key] = val
				}
				for key, val := range kv.shardstate[shard].ClientState {
					args.ShardInfo[shard].ClientState[key] = val
				}
			}

			for _, server := range newCon.Groups[desGid] {
				n = kv.catchup()
				kv.startPaxos(Op{}, n)
				for _, shard := range shards {
					args.ShardInfo[shard] = ShardsDB{
						Database:    make(map[string]string),
						ClientState: make(map[int64]string),
					}
					args.Shard = shard

					for key, val := range kv.shardstate[shard].Database {
						args.ShardInfo[shard].Database[key] = val
					}
					for key, val := range kv.shardstate[shard].ClientState {
						args.ShardInfo[shard].ClientState[key] = val
					}
				}
				var reply TransferShardsReply
				kv.mu.Unlock()
				if Debug > 0 {
					fmt.Printf("...kvServer...[GID %d][Server %d] sending the database  %v to %d:%s for config %d\n", kv.gid, kv.me, args.ShardInfo, desGid, server, args.Nconfig)
				}
				ok := call(server, "ShardKV.ReceiveShard", args, &reply)
				kv.mu.Lock()
				if ok {
					//do something
					if reply.Err == ErrNotReady {
						break
					} else if reply.Err == OK || reply.Err == ErrDuplicate {
						done = true
						break
					}

				} else {
					//try another server
				}
				time.Sleep(40 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) reconfig(New_Config shardmaster.Config) {

	prevConfig := kv.config
	for i := kv.config.Num + 1; i <= New_Config.Num; i++ {
		//check if I can do the transfer
		done := true
		for shard, gid := range prevConfig.Shards {
			if gid == kv.gid && !kv.availableShard[prevConfig.Num][shard] {
				done = false
				break
			}
		}
		if !done {
			DPrintf("[GID %d][Server %d]can't do the transfer op. not received enough shard yet %v, %v\n", kv.gid, kv.me, prevConfig.Shards, kv.availableShard[prevConfig.Num])
			return
		}
		new_config := kv.sm.Query(i)
		//make the shard unavailable

		n := kv.catchup()
		kv.startPaxos(Op{}, n)

		kv.giveShard(prevConfig, new_config)
		//now update the paxos
		updateConfig := Op{}
		updateConfig.PrevConfig = prevConfig
		updateConfig.NewConfig = new_config
		updateConfig.ReconfigID = nrand()

		nx := kv.catchup()
		if kv.config.Num < new_config.Num {
			kv.startPaxos(updateConfig, nx)
			kv.handleUpdate(updateConfig)
		}
		prevConfig = new_config
	}
	DPrintf("[GID %d][Server %d] I am done tranferring shards and My config is %d\n", kv.gid, kv.me, kv.config.Num)

}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.catchup()
	new_config := kv.sm.Query(-1)
	if kv.config.Num < new_config.Num {
		kv.configChange = true
		kv.reconfig(new_config)
	}
	kv.configChange = false

}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid

	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.shardstate = make(map[int]ShardsDB)
	kv.availableShard = make(map[int][10]bool)
	kv.extraInfo = make(map[int64]string)
	kv.configChange = false
	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
