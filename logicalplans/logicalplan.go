package logicalplans

import (
	"fmt"

	"github.com/fastbyt3/query-engine/datatypes"
)

type LogicalPlan interface {
	Schema() datatypes.Schema
	Children() []LogicalPlan
	String() string
}

func Pprint(lp LogicalPlan, indent int) string {
	str := ""
	str += fmt.Sprintf("%s\n", lp)

	for _, child := range lp.Children() {
		for i := 0; i < indent; i++ {
			str += " "
		}
		str += Pprint(child, indent)
	}

	return str
}
