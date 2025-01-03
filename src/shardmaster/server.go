package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
	"time"
)

const (
	JOIN  = "join"
	LEAVE = "leave"
	MOVE  = "move"
	QUERY = "query"
	DEBUG = 0
)

type ShardState struct {
	gid int64
	val int
}
type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config      // indexed by config num
	state   map[int64]int //random --> config number if already has been applied
	/*

		const NShards = 10

		type Config struct {
		  Num int // config number
		  Shards [NShards]int64 // gid
		  Groups map[int64][]string // gid -> servers[]
		}

	*/
}

type Op struct {
	// Your data here.
	Gid       int64
	Servers   []string
	Rand      int64 //to have the unique ops
	Operation string
	Shards    int
	Num       int //this will most probalby for the query
	Shard     int // this is for move
}

func (sm *ShardMaster) waitUntilDone(n int) Op {
	t := 10 * time.Millisecond
	time.Sleep(t)
	for {
		dc, vals := sm.px.Status(n)
		/* if Debug > 0 {
		   fmt.Printf("...kvServer..[Server %d],my write op for seq# %d is %t, %v\n",kv.me, n, dc,vals)
		 }*/
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

func countGroup(nC Config) map[int64]int {
	config := make(map[int64]int)
	for key, _ := range nC.Groups {
		config[key] = 0
	}

	for _, gid := range nC.Shards {
		config[gid] += 1
	}
	return config
}
func findMinGid(count map[int64]int) ShardState {
	maxVal := math.MaxInt32
	res := ShardState{}
	//var minGid int64

	for key, val := range count {
		if val < maxVal {
			res.gid = key
			maxVal = val
			res.val = maxVal
		}
	}
	return res
}
func (sm *ShardMaster) handleMove(op Op) {
	num := len(sm.configs)
	last_config := sm.configs[num-1] // get the last_config seen so far
	new_config := Config{
		Num:    last_config.Num + 1,
		Shards: last_config.Shards,
		Groups: make(map[int64][]string),
	}
	for key, val := range last_config.Groups {
		new_config.Groups[key] = val
	}
	new_config.Shards[op.Shard] = op.Gid
	sm.configs = append(sm.configs, new_config)
}
func (sm *ShardMaster) handleLeave(op Op) {
	num := len(sm.configs)
	last_config := sm.configs[num-1] // get the last_config seen so far
	new_config := Config{
		Num:    last_config.Num + 1,
		Shards: last_config.Shards,
		Groups: make(map[int64][]string),
	}
	//copying the groups and the
	count := 0
	/*this for looop takes care of copying from existing replica groups to the new one*/
	for key, val := range last_config.Groups {
		new_config.Groups[key] = val
		count++ // to check of how many groups are there
	}
	_, ex := new_config.Groups[op.Gid]
	if !ex {
		sm.configs = append(sm.configs, new_config)
		return
	} else {
		delete(new_config.Groups, op.Gid) //deleting the exisitng group
		count--                           //derementing the number of groups
	}
	if DEBUG > 0 {
		fmt.Printf("from leave [Server %d] --> the number of grops are %d\n", sm.me, count)
	}
	if count <= 0 {
		//indicates no group will be here after leaving
		for i, _ := range new_config.Shards {
			new_config.Shards[i] = 0
		}

	} else {
		en_config := countGroup(new_config)
		en_config[op.Gid] = math.MaxInt32
		for shard, gid := range new_config.Shards {
			if gid == op.Gid {
				//find the minimum amount of the member
				res1 := findMinGid(en_config)
				new_config.Shards[shard] = res1.gid
				en_config[res1.gid]++
			}

		}

	}
	sm.configs = append(sm.configs, new_config)

}
func (sm *ShardMaster) handleJoin(op Op) {
	/*
									  first get the latest configuration
								    create a new configuration
						        then count the number of groupIds
				            if the count == 1
				            assign all of the shards to the new group.
				            if not, found the minmum group Id which has the less shard
				            in this case the newer one is the minimum one
		                then get some of the shards from the gids and put to the newer one
		                if 10 // 3 = 3 and mod 1 , one server will have extra shards 3 3 4
		                what if we have 10//4 = 2 + 2(mod) then we will need to  3 3 4  --> 3 3 3 1 --> 2 3 3 2 probably the rtrasfer will be max to min



	*/
	num := len(sm.configs)
	last_config := sm.configs[num-1] // get the last_config seen so far
	new_config := Config{
		Num:    last_config.Num + 1,
		Shards: last_config.Shards,
		Groups: make(map[int64][]string),
	}
	//copying the groups and the
	count := 0
	/*this for looop takes care of copying from existing replica groups to the new one*/
	for key, val := range last_config.Groups {
		new_config.Groups[key] = val
		count++ // to check of how many groups are there
	}
	if DEBUG > 0 {
		fmt.Printf("[SERVER %d]from join --> the number of grops are  %d\n", sm.me, count)
	}
	_, e := new_config.Groups[op.Gid]
	if !e {
		count++
	}

	//count++                                //this is for total number of groups including the new one
	new_config.Groups[op.Gid] = op.Servers // assiging itself as the new group
	if count == 1 {
		/*this is for the edge case if there is only one group available*/
		for i, _ := range new_config.Shards {
			new_config.Shards[i] = op.Gid
		}
	} else {
		num_shards := make(map[int64]int)   //keep track of current num_shards per gid
		en_config := countGroup(new_config) //existing groups and there total number of shards to keep track of which one is minimal
		if !e {
			en_config[op.Gid] = 0
		} // for new one this is zero
		equal_shards := NShards / count
		for shard, gid := range new_config.Shards {
			val, ex := num_shards[gid]
			if ex {
				if val+1 > equal_shards {
					//get the minimum shardfs
					res1 := findMinGid(en_config)

					if res1.val >= equal_shards {
						//we don't want to move if min shard is satteled already
						num_shards[gid]++
						continue
					}
					new_config.Shards[shard] = res1.gid
					num_shards[res1.gid]++
					en_config[gid]--
					en_config[res1.gid]++
				} else {
					num_shards[gid]++
				}

			} else {
				//not exist
				num_shards[gid] = 1
			}
		}

	}
	sm.configs = append(sm.configs, new_config)

}
func (sm *ShardMaster) handlePreiousSEQ(op Op) {
	_, ex := sm.state[op.Rand]
	if ex {
		return
	}
	if op.Operation == JOIN {
		sm.handleJoin(op)
		//handle this
	} else if op.Operation == LEAVE {
		sm.handleLeave((op))
	} else if op.Operation == MOVE {
		sm.handleMove(op)
	}
	sm.state[op.Rand] = 1

}

func (sm *ShardMaster) apply_all_op(n_next int) {
	n_prev := sm.px.PeerDone[sm.me] + 1 // or possibly track this as kv.lastAppliedSeq if available
	if DEBUG > 0 {
		fmt.Printf("...Shardmaster..[Server %d],the min from paxos is %d and max is %d\n", sm.me, n_prev, n_next)
	}

	op := Op{}
	for i := n_prev; i <= n_next; i++ {
		sm.px.Start(i, op)
		recvd_val := sm.waitUntilDone(i)
		sm.handlePreiousSEQ(recvd_val)
		if DEBUG > 0 {
			fmt.Printf("...ShardMaster..[Server %d],checking decided value for %d\n", sm.me, i)
			for j, val := range sm.configs {
				fmt.Printf("...ShardMaster..[Server %d],config for index %d is %v\n", sm.me, j, val)
			}
		}
	}

}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	join := Op{}
	join.Rand = nrand()
	join.Servers = args.Servers
	join.Gid = args.GID
	join.Operation = JOIN
	n := sm.px.Max()

	sm.apply_all_op(n)

	next_max := n + 1

	for {
		sm.px.Start(next_max, join)
		recvd_val := sm.waitUntilDone(next_max)
		if recvd_val.Rand == join.Rand {

			sm.handlePreiousSEQ(recvd_val)
			if DEBUG > 0 {
				fmt.Printf("from Join --> matched paxos on ops %s\n", recvd_val.Operation)
			}

			break
		}
		sm.handlePreiousSEQ(recvd_val)
		next_max++
	}
	sm.px.Done(next_max - 1)
	if DEBUG > 0 {
		for j, val := range sm.configs {
			fmt.Printf("...ShardMaster..[Server %d],config for index %d is %v\n", sm.me, j, val)
		}
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	join := Op{}
	join.Rand = nrand()
	//join.Servers = args.Servers
	join.Gid = args.GID
	join.Operation = LEAVE
	n := sm.px.Max()

	sm.apply_all_op(n)

	next_max := n + 1

	for {
		sm.px.Start(next_max, join)
		recvd_val := sm.waitUntilDone(next_max)
		if recvd_val.Rand == join.Rand {

			sm.handlePreiousSEQ(recvd_val)
			if DEBUG > 0 {
				fmt.Printf("from Join --> matched paxos on ops %s\n", recvd_val.Operation)
			}
			break
		}
		sm.handlePreiousSEQ(recvd_val)
		next_max++
	}
	sm.px.Done(next_max - 1)
	if DEBUG > 0 {
		for j, val := range sm.configs {
			fmt.Printf("...ShardMaster..[Server %d],config for index %d is %v\n", sm.me, j, val)
		}
	}
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	join := Op{}
	join.Rand = nrand()
	//join.Servers = args.Servers
	join.Shard = args.Shard
	join.Gid = args.GID
	join.Operation = MOVE
	n := sm.px.Max()

	sm.apply_all_op(n)

	next_max := n + 1

	for {
		sm.px.Start(next_max, join)
		recvd_val := sm.waitUntilDone(next_max)
		if recvd_val.Rand == join.Rand {
			if DEBUG > 0 {
				fmt.Printf("from Join --> matched paxos on ops %s\n", recvd_val.Operation)
			}
			sm.handlePreiousSEQ(recvd_val)
			break
		}
		sm.handlePreiousSEQ(recvd_val)
		next_max++
	}
	sm.px.Done(next_max - 1)
	if DEBUG > 0 {
		for j, val := range sm.configs {
			fmt.Printf("...ShardMaster..[Server %d],config for index %d is %v\n", sm.me, j, val)
		}
	}

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	query := Op{}
	query.Rand = nrand()
	//join.Servers = args.Servers
	query.Num = args.Num
	query.Operation = QUERY
	new_max := sm.px.Max()
	sm.apply_all_op(new_max)

	next_max := new_max + 1
	for {
		sm.px.Start(next_max, query)
		recvd_val := sm.waitUntilDone(next_max)
		if recvd_val.Rand == query.Rand {
			if DEBUG > 0 {
				fmt.Printf("from Join --> matched paxos on ops %s\n", recvd_val.Operation)
			}
			sm.handlePreiousSEQ(recvd_val)
			break
		}
		sm.handlePreiousSEQ(recvd_val)
		next_max++
	}
	sm.px.Done(next_max - 1)

	n := len(sm.configs)
	//fmt.Printf("the len of conf is %d\n", n)

	// Your code here.
	if args.Num == -1 || args.Num >= n {
		reply.Config = sm.configs[n-1]

	} else {
		reply.Config = sm.configs[args.Num]
	}
	if DEBUG > 0 {
		fmt.Printf("Query -->Server[%d] the group is %v %d %d\n", sm.me, reply.Config, n, args.Num)
	}

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.state = make(map[int64]int)
	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
