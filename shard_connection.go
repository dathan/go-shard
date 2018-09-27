package goshard

import "fmt"

// define an interface to return another interface
type ShardConfig interface {
	GetShardLookup() ShardLookup
	NewShardConnection(uint64) (error, *ShardConnection)
	NewShardConnectionByShardId(uint) (error, *ShardConnection)
}

// this holds the Shard connection
type ShardConnection struct {
	Connection
	ShardId uint
}

// our test type imp the ShardConfig interface
type ShotsShardConfig struct{}

func (s ShotsShardConfig) GetShardLookup() ShardLookup {

	var cp []*ConnectionParams
	var i uint = 1
	for ; i <= 55; i++ {
		cp = append(cp, &ConnectionParams{
			Host:     "localhost",
			Dbname:   fmt.Sprintf("usershard%d", i),
			User:     "hanksapi",
			Password: "H@nk$@p11p@$kn@H",
			ShardId:  i,
		})
	}

	rsl := NewShardLookup(cp)
	return rsl

}

//set up all the hosts
func (sconf ShotsShardConfig) NewShardConnection(entity_id uint64) (error, *ShardConnection) {

	rsl := sconf.GetShardLookup()

	err, c := rsl.Lookup(entity_id)
	if err != nil {
		return err, nil
	}

	err, rc := NewConnection(c)
	sc := &ShardConnection{*rc, c.ShardId}

	return err, sc

}

//set up all the hosts
func (sconf ShotsShardConfig) NewShardConnectionByShardId(shard_id uint) (error, *ShardConnection) {

	rsl := sconf.GetShardLookup()
	cs := rsl.GetAll()
	shard_id = shard_id - 1 // 0 based
	c := cs[shard_id]
	err, rc := NewConnection(c)
	sc := &ShardConnection{*rc, c.ShardId}

	return err, sc

}
