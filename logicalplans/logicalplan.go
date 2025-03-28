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

func PprintPlan(lp LogicalPlan, spaces int) string {
	return format(lp, spaces, 0)
}

func format(lp LogicalPlan, spaces, indentLevel int) string {
	str := ""

	for range indentLevel {
		for range spaces {
			str += " "
		}
	}

	str += fmt.Sprintf("%s\n", lp)

	for _, child := range lp.Children() {
		str += format(child, spaces, indentLevel+1)
	}

	return str
}
