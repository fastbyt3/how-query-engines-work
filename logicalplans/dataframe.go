package logicalplans

import (
	"github.com/fastbyt3/query-engine/datatypes"
)

type Dataframe interface {
	// Apply projection
	Project(expr []LogicalExpr) Dataframe

	// Apply filter
	Filter(expr LogicalExpr) Dataframe

	// Aggregate
	Aggregate(groupBy []LogicalExpr, aggregateExpr []AggregateExpr) Dataframe

	// Schema of data produced by this Dataframe
	Schema() datatypes.Schema

	// Get logical plan for dataframe
	LogicalPlan() LogicalPlan
}

type DefaultDataframe struct {
	plan LogicalPlan
}

func NewDefaultDataframe(plan LogicalPlan) *DefaultDataframe {
	return &DefaultDataframe{plan}
}

func (d DefaultDataframe) Aggregate(groupBy []LogicalExpr, aggregateExpr []AggregateExpr) Dataframe {
	return DefaultDataframe{NewAggregate(d.plan, groupBy, aggregateExpr)}
}

func (d DefaultDataframe) Filter(expr LogicalExpr) Dataframe {
	return DefaultDataframe{NewSelection(d.plan, expr)}
}

func (d DefaultDataframe) LogicalPlan() LogicalPlan {
	return d.plan
}

func (d DefaultDataframe) Project(expr []LogicalExpr) Dataframe {
	return DefaultDataframe{NewProjection(d.plan, expr)}
}

func (d DefaultDataframe) Schema() datatypes.Schema {
	return d.plan.Schema()
}

var _ Dataframe = (*DefaultDataframe)(nil)
