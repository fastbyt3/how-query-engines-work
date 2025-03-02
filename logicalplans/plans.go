package logicalplans

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fastbyt3/query-engine/datasources"
)

// Scan is for fetching data from a datasource.
//
// Its the only logical plan that doesn't take an input logical plan
// making it a leaf node in query tree
type Scan struct {
	Path        string
	Datasource  datasources.DataSource
	Projections []string
}

func NewScan(path string, datasource datasources.DataSource, projections []string) Scan {
	return Scan{path, datasource, projections}
}

func (s *Scan) String() string {
	if len(s.Projections) == 0 {
		return fmt.Sprintf("Scan: %s; path=None", s.Path)
	}
	return fmt.Sprintf("Scan: %s; path=%s", s.Path, s.Projections)
}

func (s *Scan) deriveSchema() arrow.Schema {
	schema := s.Datasource.Schema()
	if len(s.Projections) == 0 {
		return schema
	}

	var fields []arrow.Field

	for _, name := range s.Projections {
		for _, f := range schema.Fields() {
			if name == f.Name {
				fields = append(fields, f)
			}
		}
	}

	return *arrow.NewSchema(fields, nil)
}

func (s *Scan) Children() []LogicalPlan {
	return []LogicalPlan{}
}

func (s *Scan) Schema() arrow.Schema {
	return s.deriveSchema()
}

var _ LogicalPlan = (*Scan)(nil)

// Projection applies a series of logical expressions to its input.
//
// Examples: `SELECT a, b, c FROM foo`
//
// A complex example would be: `SELECT (CAST(a AS float) * 3.14) AS my_float FROM foo`
type Projection struct {
	Input LogicalPlan
	Exprs []LogicalExpr
}

func NewProjection(input LogicalPlan, exprs []LogicalExpr) Projection {
	return Projection{input, exprs}
}

func (p *Projection) Children() []LogicalPlan {
	return []LogicalPlan{p.Input}
}

func (p *Projection) Schema() arrow.Schema {
	var fields []arrow.Field
	for _, e := range p.Exprs {
		fields = append(fields, e.ToField(p.Input))
	}
	return *arrow.NewSchema(fields, nil)
}

// String implements LogicalPlan.
func (p *Projection) String() string {
	s := make([]string, len(p.Exprs))
	for i, e := range p.Exprs {
		s[i] = e.String()
	}
	return fmt.Sprintf("Projection: %s", strings.Join(s, ", "))
}

var _ LogicalPlan = (*Projection)(nil)

// Selection aka Filter, decides which rows to be included in output
//
// In SQL, this is the `WHERE` clause. The result of this plan should always result in Boolean
type Selection struct {
	Input LogicalPlan
	Expr  LogicalExpr
}

func NewSelection(input LogicalPlan, expr LogicalExpr) Selection {
	return Selection{input, expr}
}

func (s *Selection) Children() []LogicalPlan {
	return []LogicalPlan{s.Input}
}

func (s *Selection) Schema() arrow.Schema {
	return s.Input.Schema()
}

func (s *Selection) String() string {
	return fmt.Sprintf("Filter: %s", s.Expr)
}

var _ LogicalPlan = (*Selection)(nil)

// Aggregate executes expressions like MIN, MAX, AVG and SUM on underlying data
//
// example: `SELECT job_title, AVG(salary) FROM emp_grp GROUP BY job_title`
type Aggregate struct {
	Input          LogicalPlan
	GroupExprs     []LogicalExpr
	AggregateExprs []AggregateExpr
}

func NewAggregate(input LogicalPlan, groupExpr []LogicalExpr, aggregateExpr []AggregateExpr) Aggregate {
	return Aggregate{input, groupExpr, aggregateExpr}
}

func (a *Aggregate) Children() []LogicalPlan {
	return []LogicalPlan{a.Input}
}

func (a *Aggregate) Schema() arrow.Schema {
	fields := make([]arrow.Field, len(a.AggregateExprs)+len(a.GroupExprs))

	for i, e := range a.GroupExprs {
		fields[i] = e.ToField(a.Input)
	}

	j := 0
	for i := len(a.GroupExprs); i < len(a.AggregateExprs); i++ {
		fields[i] = a.AggregateExprs[j].ToField(a.Input)
		j++
	}

	return *arrow.NewSchema(fields, nil)
}

func (a *Aggregate) String() string {
	return fmt.Sprintf("Aggregate: groupExpr=%v, aggregateExprs=%v", a.GroupExprs, a.AggregateExprs)
}

var _ LogicalPlan = (*Aggregate)(nil)
