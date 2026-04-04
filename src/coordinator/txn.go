package coordinator

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"6.5840/kvraft"
	"6.5840/porcupine"
	"6.5840/shardkv"
)

type TxnModelInput struct {
	TxnId string
	Ops   []TxnOperation
}

type TxnModelOutput struct {
	Err    shardkv.Err
	Values []string
}

var TxnModel = porcupine.Model{
	Partition:      porcupine.NoPartition,
	PartitionEvent: porcupine.NoPartitionEvent,
	Init: func() interface{} {
		return map[string]string{}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(map[string]string)
		inp := input.(TxnModelInput)
		out := output.(TxnModelOutput)
		next := cloneTxnState(st)

		if out.Err == shardkv.ErrTxnAborted {
			return true, next
		}
		if out.Err != shardkv.OK || len(out.Values) != len(inp.Ops) {
			return false, state
		}
		for i, op := range inp.Ops {
			cur := next[op.Key]
			switch op.Op {
			case kvraft.GetOp:
				if out.Values[i] != cur {
					return false, state
				}
			case kvraft.PutOp:
				next[op.Key] = op.Value
				if out.Values[i] != op.Value {
					return false, state
				}
			case kvraft.AppendOp:
				next[op.Key] = cur + op.Value
				if out.Values[i] != next[op.Key] {
					return false, state
				}
			default:
				panic("invalid op")
			}
		}
		return true, next
	},
	Equal: func(state1, state2 interface{}) bool {
		return reflect.DeepEqual(state1, state2)
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(TxnModelInput)
		out := output.(TxnModelOutput)
		parts := make([]string, 0, len(inp.Ops))
		for _, op := range inp.Ops {
			switch op.Op {
			case kvraft.GetOp:
				parts = append(parts, fmt.Sprintf("Get(%q)", op.Key))
			case kvraft.PutOp:
				parts = append(parts, fmt.Sprintf("Put(%q,%q)", op.Key, op.Value))
			case kvraft.AppendOp:
				parts = append(parts, fmt.Sprintf("Append(%q,%q)", op.Key, op.Value))
			}
		}
		return fmt.Sprintf("%s:[%s] -> %s %s", inp.TxnId, strings.Join(parts, ", "), out.Err, fmt.Sprintf("[%s]", strings.Join(out.Values, ", ")))
	},
	DescribeState: func(state interface{}) string {
		st := state.(map[string]string)
		keys := make([]string, 0, len(st))
		for key := range st {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, fmt.Sprintf("%q:%q", key, st[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	},
}

func cloneTxnState(st map[string]string) map[string]string {
	next := make(map[string]string, len(st))
	for key, value := range st {
		next[key] = value
	}
	return next
}
