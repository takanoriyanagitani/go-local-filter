package local

type Bucket struct {
	name string
}

func BucketNew(name string) Bucket {
	return Bucket{name}
}

func (b Bucket) AsString() string { return b.name }
