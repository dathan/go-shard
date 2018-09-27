package goshard

import "testing"

func TestAsyncShardConnection_SelectAll(t *testing.T) {
	s := ShotsShardConfig{}
	as := NewAsyncShardConnection(s)
	srs := as.SelectAll("SELECT COUNT(*) as cnt FROM sessions")
	for _, result := range srs {
		row := result.rows
		if len(row) > 0 {
			r := row[0]["cnt"]
			t.Logf("SHARDID: %d COUNT: %s\n", result.shard_id, r)
		} else {
			t.Logf("no results for %d\n", result.shard_id)
		}
	}
}

func TestAsyncShardConnection_SomeSelectAll(t *testing.T) {
	s := ShotsShardConfig{}
	as := NewAsyncShardConnection(s)
	//as.Shards = []uint{4,40,8,11,17}

	srs := as.SelectAll("SELECT count(*) as cnt FROM sessions")
	for _, result := range srs {
		row := result.rows
		if len(row) > 0 {
			r := row[0]["cnt"]
			t.Logf("SHARDID: %d COUNT: %s\n", result.shard_id, r)
		} else {
			t.Logf("no results for %d -> %s\n", result.shard_id, result.error)
		}
	}
}
