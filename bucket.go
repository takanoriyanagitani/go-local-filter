package local

// Bucket contains a bucket name.
type Bucket struct{ name string }

// BucketNew creates a bucket from a checked name.
//
// # Arguments
//   - name: A checked name(this method has no validation logic).
func BucketNew(name string) Bucket { return Bucket{name} }

// AsString returns a raw bucket string name.
func (b Bucket) AsString() string { return b.name }
