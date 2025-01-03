package paxos
const (
	PROPOSE = "PROPOSE"
	ACCEPT = "ACCEPT"
	DONE = "DONE"
	DECIDE = "DECIDE"
	PROPOSE_OK = "PROPOSE-OK"
	ACCEPT_OK = "ACCEPT-OK"
	PROPOSE_REJECT = "PROPOSE_REJECT"
	ACCEPT_REJECT = "ACCEPT_REJECT"
	DECIDE_REJECT = "DECIDE_REJECT"
	NETWORK_ERROR = "NETWORK_ERROR"
)
type Task string
type Taskreply string
type ProposerArgs struct {
	N float64 //new proposal seq that will be sen to the acceptors
	Task Task
	Seq int
	V interface{}
	Min int
	Caller int
}
type AcceptorReply struct {
	N_a float64 //last highest accepted sequence
	V_a interface{} //last highest accpeted value
	Taskreply Taskreply
	Seq int
	N_p float64 //highest accepted sequence
	Min int
	Callee int

}
