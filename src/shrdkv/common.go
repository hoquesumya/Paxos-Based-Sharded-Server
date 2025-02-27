package shardkv

import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
	ErrDuplicate  = "ErrDup"
)

type ShardsDB struct {
	Database    map[string]string
	ClientState map[int64]string
}

type Err string

type PutArgs struct {
	Key     string
	Value   string
	DoHash  bool // For PutHash
	CientID int64
	Nconfig int
	Shard   int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash

}

type GetArgs struct {
	Key     string
	CientID int64
	Nconfig int
	Shard   int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferShardsArgs struct {
	ShardInfo map[int]ShardsDB
	Gid       int64
	Shard     int
	Nconfig   int
}
type TransferShardsReply struct {
	Err Err
	//Databse []TempDb
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
