package poled

import (
	"testing"
)

func mustNewPoled() *Poled {
	conf := DefaultConfig()
	conf.IndexPath="/var/www/go/pole/tests/indexes"
	pd, err := NewPoled(conf)
	if err != nil {
		panic(err)
	}
	return pd
}

func TestNewPoled(t *testing.T) {
	pd := mustNewPoled()
	if pd == nil {
		t.Fatal("init Poled failed")
	}
}

func TestExec(t *testing.T) {
	pd := mustNewPoled()
	if pd == nil {
		t.Fatal("init Poled failed")
	}

	tests := []struct {
		name string
		want error
		sql  string
	}{
		{
			name: "create",
			sql:  "create table test (id int(10) not null,name varchar(255) not null)",
			want: nil,
		},
		{
			name: "insert",
			sql:  "insert into test (id,name) values (1,'hello'),(2,'world')",
			want: nil,
		},
		{
			name: "insert1",
			sql:  "insert into test set id=3,name='hello'",
			want: nil,
		},
		{
			name: "insert2",
			sql:  "insert into test set id=3,name='hello'",
			want: nil,
		},
		{
			name: "update",
			sql:  "update test set name='haha' where id=2",
			want: nil,
		},
		{
			name: "select",
			sql:  "select * from test where name='hello'",
			want: nil,
		},
		{
			name: "update1",
			sql:  "update test set name='haaaa' where id=2",
			want: nil,
		},
		{
			name: "delete",
			sql:  "delete from test where id=1",
			want: nil,
		},
		{
			name: "select2",
			sql:  "select * from test where name like 'h*'",
			want: nil,
		},
		{
			name: "select3",
			sql:  "select * from test where name='hello'",
			want: nil,
		},
		{
			name: "select3",
			sql:  "select * from test where id=2",
			want: nil,
		},
		{
			name: "select-in",
			sql:  "select * from test where id in(1,2,3)",
			want: nil,
		},
		{
			name: "drop",
			sql:  "drop table test",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := pd.Exec(tt.sql)
			if rs.Error() != tt.want {
				t.Logf("Poled.Exec() error = %v, want %v", rs.Error(), tt.want)
				t.Fail()
			} else {
				t.Log(pd.meta.All())
			}
		})
	}
}
