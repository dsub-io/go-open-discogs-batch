package result

type result struct {
	count int
	err   error
}

func (r *result) Count() int {
	return r.count
}

func (r *result) Err() error {
	return r.err
}

func (r *result) IsErr() bool {
	return r.err != nil
}

func (r *result) Sum(that Result) Result {
	if that == nil {
		return r
	}
	r.count += that.Count()
	if r.err == nil {
		r.err = that.Err()
	}
	return r
}

func NewResult(count int, err error) Result {
	return &result{count, err}
}

type Result interface {
	Count() int
	Err() error
	IsErr() bool
	Sum(Result) Result
}
