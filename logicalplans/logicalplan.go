package logicalplans

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

type LogicalPlan interface {
	Schema() arrow.Schema
	Children() []LogicalPlan
	String() string
}

func Pprint(lp LogicalPlan, indent int) string {
	str := ""

	for i := 0; i < indent; i++ {
		str += " "
	}

	str += fmt.Sprintf("%s\n", lp)

	for _, child := range lp.Children() {
		str += Pprint(child, indent)
	}

	return str
}
