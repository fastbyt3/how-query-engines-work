package datatypes

import "github.com/apache/arrow-go/v18/arrow"

type Schema struct {
	arrow.Schema
}

func NewSchema(fields []arrow.Field) *Schema {
	return &Schema{*arrow.NewSchema(fields, nil)}
}

func (s *Schema) Select(names []string) (Schema, []int) {
	if len(names) == 0 {
		indices := make([]int, len(s.Fields()))
		for i := 0; i < len(s.Fields()); i++ {
			indices[i] = i
		}
		return *s, indices
	}

	var fields []arrow.Field
	var indices []int
	for _, name := range names {
		for idx, f := range s.Fields() {
			if name == f.Name {
				fields = append(fields, f)
				indices = append(indices, idx)
			}
		}
	}

	return Schema{*arrow.NewSchema(fields, nil)}, indices
}
