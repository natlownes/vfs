// The S3 vistar-test bucket layout must be exactly as specified below,
// or else calls to Readdir in the various tests will fail. (This
// affects the Mem and OS tests as well, but doesn't really matter for
// them b/c those filesystems are not persistent across tests.) If you
// get a large number of failures, first check that the S3 bucket
// contents are as shown below.
//
//  - s3://vistar-test
//    + directory/
//    | + sub_directory/
//    | + child.txt content:(hi, child)
//    + empty_directory/
//    + large_directory/
//    | + 0001 (empty file)
//    | + 0002 (empty file)
//    | ...(continues until)
//    | + 1100 (empty file)
//    + root.txt content:(hi, root)
//    + stat_test/
//    + stat_test1/
package integration

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/natlownes/vfs"
	"github.com/natlownes/vfs/s3fs"
)

type S3FsProvider struct {
	fs FileSystem
}

func s3Session() *session.Session {
	return session.New(aws.NewConfig().WithRegion("us-east-1"))
}

func (s3p *S3FsProvider) Setup() {
	s3p.fs = s3fs.New(s3Session(), "vistar-test", s3fs.ACL("private"))
}

func (*S3FsProvider) Name() string {
	return "S3Fs"
}

func (s3p *S3FsProvider) Create() FileSystem {
	return s3p.fs
}

var provider = &S3FsProvider{}

func createAndHeadObject(
	client *s3.S3, fs FileSystem, key string) *s3.HeadObjectOutput {

	bucket := "vistar-test"
	w, err := fs.Create(key)
	Expect(err).ToNot(HaveOccurred())
	defer fs.Remove(key)
	err = w.Close()
	Expect(err).ToNot(HaveOccurred())
	out, err := client.HeadObject(&s3.HeadObjectInput{
		Key:    &key,
		Bucket: &bucket,
	})
	Expect(err).ToNot(HaveOccurred())
	return out
}

func S3(fsp FSProvider) bool {
	once := &setupOnce{fsp: fsp}

	var _ = Describe("S3 specific features", func() {

		var (
			fs       FileSystem
			s3Client *s3.S3
		)

		s3Client = s3.New(s3Session())
		fs = once.Get()

		It("should guess mime types from key ending in '.html'", func() {
			Expect(createAndHeadObject(s3Client, fs, "s3fs/index.html").ContentType).
				To(Equal(aws.String("text/html; charset=utf-8")))
		})

		It("should guess mime types from key ending in '.js'", func() {
			Expect(createAndHeadObject(s3Client, fs, "s3fs/app.js").ContentType).
				To(Equal(aws.String("application/javascript")))
		})

		It("should guess mime types from key ending in '.css'", func() {
			Expect(createAndHeadObject(s3Client, fs, "s3fs/app.css").ContentType).
				To(Equal(aws.String("text/css; charset=utf-8")))
		})

		It("should guess mime types from key ending in '.jpg'", func() {
			Expect(createAndHeadObject(s3Client, fs, "s3fs/jokes.jpg").ContentType).
				To(Equal(aws.String("image/jpeg")))
		})
	})

	return true
}

var _ = All(provider)
var _ = S3(provider)
