package goshard

import (
	"context"
	"reflect"
	"time"
)

// when you need to query all/multiple the shards at the same time.
type AsyncShardConnection struct {
	Shards  []uint                // which shards to query if not set query them all
	conns   []ShardConnection     // connections of the shards
	sconf   ShardConfig           // The interface which is implemented
	sr      chan AsyncShardResult // the reciever channel for results
	timeout time.Duration
}

// The results can be rows
type AsyncShardResult struct {
	rows     []Row
	shard_id uint
	error    error
}

// every query needs a conn, a string and a query. Wrap in a channel for timeout
type CommonQueryArgs struct {
	sr    chan AsyncShardResult
	conn  ShardConnection
	query string
	args  []interface{}
	call  func(c CommonQueryArgs) // there must be a better way to do this without reflection
}

// build a new asyncShard Connection
func NewAsyncShardConnection(s ShardConfig) *AsyncShardConnection {

	return &AsyncShardConnection{sconf: s}
}

// set the timeout
func (a *AsyncShardConnection) TimeOut(d time.Duration) {
	a.timeout = d
}

// make sure the channel is reset because it could be closed
func (a *AsyncShardConnection) resetChannel() {
	a.sr = make(chan AsyncShardResult, 25)
}

// go through connection_params and make connections
func (a *AsyncShardConnection) buildConnections() {
	//todo clean this up
	if len(a.Shards) == 0 {
		sl := a.sconf.GetShardLookup()
		for _, shard_cp := range sl.GetAll() {
			_, c := NewConnection(shard_cp)
			a.conns = append(a.conns, ShardConnection{Connection: *c, ShardId: shard_cp.ShardId})
		}
	} else {
		sl := a.sconf.GetShardLookup()
		for _, shard_cp := range sl.GetAll() {
			if ok, _ := in_array(shard_cp.ShardId, a.Shards); ok {
				_, c := NewConnection(shard_cp)
				a.conns = append(a.conns, ShardConnection{Connection: *c, ShardId: shard_cp.ShardId})
			}
		}
	}

}

// get all Shard Results
func (a *AsyncShardConnection) SelectAll(query string, args ...interface{}) []AsyncShardResult {

	a.buildConnections()
	a.resetChannel()

	// lets cancel requests if they exceed timeout -- todo think about context.WithTimeout()
	ctx, cancel := context.WithCancel(context.Background())
	// fanout the queries
	for _, conn := range a.conns {
		//launch database queries across all the shards in a fanout

		//go a.getWithTimeout(ctx,conn,query,args...)

		c := CommonQueryArgs{
			query: query,
			args:  args,
			conn:  conn,
		}
		c.call = func(c CommonQueryArgs) {

			conn := c.conn
			query := c.query
			args := c.args
			sr := c.sr

			e, rows := conn.SelectAll(query, args...)
			sr <- AsyncShardResult{
				rows:     rows,
				error:    e,
				shard_id: conn.ShardId,
			}
		}

		go a.executeTimeout(ctx, c)

	}

	// Grab all the results.
	var srs []AsyncShardResult

	var timeout <-chan time.Time
	if a.timeout.Nanoseconds() >= 1 {
		timeout = time.After(a.timeout)
	} else {
		timeout = time.After(2 * time.Second)
	}

loop: //break out the loop when a timeout is reached
	for {
		select {
		case sr := <-a.sr:
			//fmt.Printf("ADDING RESULT ShardId: %d\n", sr.shard_id)
			srs = append(srs, sr)
			if len(srs) == len(a.conns) {
				break
			}
		case <-timeout:
			//fmt.Printf("TIMEDOUT calling cancel");
			cancel()
			break loop

		default:
		}
	}

	// clean up channels
	close(a.sr)
	return srs
}

func (a *AsyncShardConnection) executeTimeout(ctx context.Context, c CommonQueryArgs) {
	var sr chan AsyncShardResult = make(chan AsyncShardResult)
	c.sr = sr

	go c.call(c)

loop2:
	for {
		select {
		case <-ctx.Done():
			// for some reason 'return' doesn't break out of the for loop
			// even though the spec says so:
			// https://golang.org/ref/spec#Return_statements
			break loop2 // for some reason return doesn't break out of the for loop

		case s := <-sr:
			a.sr <- s // send the shard result to AsyncShard
			break loop2

		default: // make non-blocking

		}
	}
	return

}

// helper method saw online
func in_array(val interface{}, array interface{}) (exists bool, index int) {
	exists = false
	index = -1

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				index = i
				exists = true
				return
			}
		}
	}

	return
}
