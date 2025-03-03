package logicalplans

import (
	"github.com/apache/arrow-go/v18/arrow"
)

type Dataframe interface {
	// Apply projection
	Project(expr []LogicalExpr) Dataframe

	// Apply filter
	Filter(expr LogicalExpr) Dataframe

	// Aggregate
	Aggregate(groupBy []LogicalExpr, aggregateExpr []AggregateExpr) Dataframe

	// Schema of data produced by this Dataframe
	Schema() arrow.Schema

	// Get logical plan for dataframe
	LogicalPlan() LogicalPlan
}

type DefaultDataframe struct {
	plan LogicalPlan
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

func (d DefaultDataframe) Schema() arrow.Schema {
	return d.plan.Schema()
}

var _ Dataframe = (*DefaultDataframe)(nil)
