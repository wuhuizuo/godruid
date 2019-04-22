package merge

import "testing"

func Test_MapCompare(t *testing.T) {
	type args struct {
		compareKeys []string
		eventA      map[string]interface{}
		eventB      map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want int8
	}{
		{
			"emptyEvent-1",
			args{
				[]string{"Key1", "Key2", "Key3"},
				map[string]interface{}{},
				map[string]interface{}{},
			},
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := MapCompare(tt.args.compareKeys, tt.args.eventA, tt.args.eventB); got != tt.want {
				t.Errorf("eventCompare() = %v, want %v", got, tt.want)
			}
		})
	}
}
