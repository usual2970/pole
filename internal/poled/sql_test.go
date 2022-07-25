package poled

import "testing"

func TestParse(t *testing.T) {
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
			sql:  "insert into test set id=1,name='hello'",
			want: nil,
		},
		{
			name:"select-all",
			sql:"select * from test where name='hello'",
			want: nil,
		},
		{
			name:"select-name",
			sql:"select name,sex from test where name='hello' and (id=1 or name=3)",
			want: nil,
		},
		{
			name: "delete",
			sql:  "delete from test where id=1",
			want: nil,
		},
		{
			name: "update",
			sql:  "update test set name='haha' where id=1",
			want: nil,
		},
		{
			name:"select-name",
			sql:"select name,sex from test where name='hello' and (id=1 or name=3)",
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
			rs, err := parse(tt.sql)
			if err != tt.want {
				t.Logf("poled.Exec() error = %v, want %v", err, tt.want)
				t.Fail()
			}
			t.Log(rs)
		})
	}
}
