package main

import (
	"fmt"

	"github.com/fastbyt3/query-engine/execution"
	"github.com/fastbyt3/query-engine/logicalplans"
)

func main() {
	ctx := execution.NewExecutionContext(1024)
	df := ctx.CSV("test-data/employee.csv")
	df = df.Filter(logicalplans.NewEqExpr(logicalplans.NewColumn("state"), logicalplans.NewLiteralString("CO")))
	df.Project([]logicalplans.LogicalExpr{
		logicalplans.NewColumn("id"),
		logicalplans.NewColumn("first_name"),
		logicalplans.NewColumn("last_name"),
		logicalplans.NewColumn("state"),
		logicalplans.NewMult(logicalplans.NewColumn("salary"), logicalplans.NewLiteralFloat(2.0)),
	})

	fmt.Println(logicalplans.PprintPlan(df.LogicalPlan(), 2))
}
