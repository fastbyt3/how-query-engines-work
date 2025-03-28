package main

import (
	"fmt"

	"github.com/fastbyt3/query-engine/execution"
	"github.com/fastbyt3/query-engine/logicalplans"
)

func main() {
	ctx := execution.NewExecutionContext(1024)
	df := ctx.CSV("test-data/sample.csv")
	df = df.Filter(
		logicalplans.NewEqExpr(
			logicalplans.NewColumn("mpg"),
			logicalplans.NewLiteralLong(21),
		),
	)
	df = df.Filter(
		logicalplans.NewEqExpr(
			logicalplans.NewColumn("cyl"),
			logicalplans.NewLiteralLong(160),
		),
	)
	df = df.Project([]logicalplans.LogicalExpr{
		logicalplans.NewColumn("id"),
		logicalplans.NewColumn("model"),
		logicalplans.NewColumn("mpg"),
	})

	fmt.Println(logicalplans.PprintPlan(df.LogicalPlan(), 2))
}
