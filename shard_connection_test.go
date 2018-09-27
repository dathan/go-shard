package goshard

import (
	"testing"
)

var result int

func TestShardConnection(t *testing.T) {

	var entity_id uint64 = 2721589
	s := ShotsShardConfig{}

	err, sc := s.NewShardConnection(entity_id)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	err, row := sc.SelectRow("SELECT * FROM user_events where user_id=? LIMIT 1", entity_id)
	if err != nil {
		t.Fatal(err)
	}

	key := "identifier"
	t.Logf("row.%s: %d on Shard: %d\n", key, row[key].(int64), sc.ShardId)
}

func TestShardConnectionLookup(t *testing.T) {
	s := ShotsShardConfig{}
	var entity_id uint64 = 17549768
	err, sc := s.NewShardConnection(entity_id)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if sc.ShardId != 10 {
		t.Fatalf("Shard is not correct")
	}

	s.NewShardConnection(1)

	entity_id = 1
	err, sc = s.NewShardConnection(entity_id)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}

	if sc.ShardId != 3 {
		t.Fatalf("Shard is not correct")
	}

}

func BenchmarkShardConnection(b *testing.B) {
	s := ShotsShardConfig{}
	for n := 0; n < b.N; n++ {
		err, sc := s.NewShardConnection(uint64(n))
		if err != nil {
			b.Fatalf("Error: %s\n", err)
		}

		err, _ = sc.SelectRow("SELECT * FROM users_counts where user_id=? LIMIT 1", n)
		if err != nil {
			b.Fatalf("Error: %s\n", err)
		}

		defer sc.Close()
		result = int(sc.ShardId)
	}
}

func BenchmarkShardConnectionSloppy(b *testing.B) {
	s := ShotsShardConfig{}
	for n := 0; n < b.N; n++ {
		err, sc := s.NewShardConnection(uint64(n))
		if err != nil {
			b.Fatalf("Error: %s\n", err)
		}

		err, _ = sc.SelectRow("SELECT * FROM users_counts where user_id=? LIMIT 1", n)
		if err != nil {
			b.Fatalf("Error: %s\n", err)
		}
	}
}

func BenchmarkShardSelect(b *testing.B) {
	s := ShotsShardConfig{}
	err, sc := s.NewShardConnection(3288897)
	if err != nil {
		b.Fatalf("Error : %s\n", err)
	}
	for n := 0; n < b.N; n++ {

		err, _ := sc.SelectRow("SELECT 1 as ping")
		if err != nil {
			b.Fatalf("Error : %s\n", err)
		}

	}
	sc.Close()
}
