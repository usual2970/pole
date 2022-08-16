package sql

import (
	"pole/internal/poled/meta"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		want error
		sql  string
	}{
		{
			name: "create",
			sql:  "create table test (id int(10) not null,Name varchar(255) not null)",
			want: nil,
		},
		{
			name: "insert",
			sql:  "insert into test (id,Name) values (1,'hello'),(2,'world')",
			want: nil,
		},
		{
			name: "insert1",
			sql:  "insert into test set id=1,Name='hello'",
			want: nil,
		},
		{
			name: "select-all",
			sql:  "select * from test where Name='hello' group by Name order by id desc limit 0 ,10",
			want: nil,
		},
		{
			name: "select-Name",
			sql:  "select Name,sex from test where Name='hello' and (id=1 or Name=3)",
			want: nil,
		},
		{
			name: "delete",
			sql:  "delete from test where id=1",
			want: nil,
		},
		{
			name: "update",
			sql:  "update test set Name='haha' where id=1",
			want: nil,
		},
		{
			name: "select-Name",
			sql:  "select Name,sex from test where Name='hello' and (id=1 or Name=3)",
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
			rs, err := Parse(tt.sql)
			if err != tt.want {
				t.Logf("poled.Exec() error = %v, want %v", err, tt.want)
				t.Fail()
			}
			t.Log(rs)
		})
	}
}

func TestBuildRequest(t *testing.T) {
	tests := []struct {
		name string
		want error
		sql  string
	}{
		{
			name: "select-all",
			sql:  "select * from test where Name=1 group by Name order by id desc limit 0 ,10",
			want: nil,
		},
		{
			name: "select-Name",
			sql:  "select Name,sex from test where Name='hello' and (id=1 or Name=3)",
			want: nil,
		},
		{
			name: "select-Name1",
			sql:  "select Name,sex from test where Name='hello' and (id=1 or Name=3)",
			want: nil,
		},
		{
			name: "select-like",
			sql:  "select Name,sex from test where Name like 'hello'",
			want: nil,
		},
		{
			name: "select-in",
			sql:  "select Name,sex from test where id in (1,2,3)",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := Parse(tt.sql)
			if err != tt.want {
				t.Logf("Parse() error = %v, want %v", err, tt.want)
				t.Fail()
			}
			req, err := rs.BuildRequest(meta.Mapping{})
			if err != tt.want {
				t.Logf("BuildRequest() error = %v, want %v", err, tt.want)
				t.Fail()
			}
			t.Log(req)
		})
	}
}
