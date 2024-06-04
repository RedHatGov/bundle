package batch

// import (
// 	"context"
// 	"os"
// 	"syscall"
// 	"testing"

// 	"github.com/docker/distribution/registry/api/errcode"

// 	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
// 	clog "github.com/openshift/oc-mirror/v2/internal/pkg/log"
// 	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
// 	"github.com/stretchr/testify/assert"
// )

// func TestWorker(t *testing.T) {

// 	log := clog.New("trace")

// 	global := &mirror.GlobalOptions{SecurePolicy: false, Quiet: false}

// 	_, sharedOpts := mirror.SharedImageFlags()
// 	_, deprecatedTLSVerifyOpt := mirror.DeprecatedTLSVerifyFlags()
// 	_, srcOpts := mirror.ImageSrcFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "src-", "screds")
// 	_, destOpts := mirror.ImageDestFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "dest-", "dcreds")
// 	_, retryOpts := mirror.RetryFlags()

// 	opts := mirror.CopyOptions{
// 		Global:              global,
// 		DeprecatedTLSVerify: deprecatedTLSVerifyOpt,
// 		SrcImage:            srcOpts,
// 		DestImage:           destOpts,
// 		RetryOpts:           retryOpts,
// 		Destination:         "oci:test",
// 		Dev:                 false,
// 		Mode:                mirror.DiskToMirror,
// 		Function:            "copy",
// 	}
// 	tempDir := t.TempDir()
// 	defer os.RemoveAll(tempDir)

// 	relatedImages := []v2alpha1.CopyImageSchema{
// 		{Source: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testa", Type: v2alpha1.TypeOCPRelease},
// 		{Source: "docker://registry/name/namespace/sometestimage-b@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-b@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testb", Type: v2alpha1.TypeOCPReleaseContent},
// 		{Source: "docker://registry/name/namespace/sometestimage-c@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-c@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testc", Type: v2alpha1.TypeOperatorBundle},
// 		{Source: "docker://registry/name/namespace/sometestimage-d@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-d@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testd", Type: v2alpha1.TypeOperatorCatalog},
// 		{Source: "docker://registry/name/namespace/sometestimage-e@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-e@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:teste", Type: v2alpha1.TypeCincinnatiGraph},
// 		{Source: "docker://registry/name/namespace/sometestimage-f@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-f@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testf", Type: v2alpha1.TypeGeneric},
// 		{Source: "docker://registry/name/namespace/sometestimage-g@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-g@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testg", Type: v2alpha1.TypeGeneric},
// 		{Source: "docker://registry/name/namespace/sometestimage-h@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-h@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testh", Type: v2alpha1.TypeGeneric},
// 		{Source: "docker://registry/name/namespace/sometestimage-i@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-i@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testi", Type: v2alpha1.TypeGeneric},
// 	}
// 	// this is a facade to get code coverage up
// 	t.Run("Testing Worker : should pass", func(t *testing.T) {

// 		w := New(log, tempDir, &Mirror{ForceError: nil})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: relatedImages}, opts)
// 		if err != nil {
// 			t.Fatal("should pass")
// 		}
// 		assert.Equal(t, len(relatedImages), len(copiedImages.AllImages))
// 	})

// 	t.Run("Testing Worker for delete: should pass", func(t *testing.T) {
// 		opts.Function = "delete"
// 		w := New(log, tempDir, &Mirror{ForceError: nil})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: relatedImages}, opts)
// 		if err != nil {
// 			t.Fatal("should pass")
// 		}
// 		assert.Equal(t, len(relatedImages), len(copiedImages.AllImages))
// 	})
// 	t.Run("Testing Worker : registry unauthorized : should fail fast", func(t *testing.T) {
// 		opts.Function = "copy"
// 		unauthorized := errcode.Error{Code: errcode.ErrorCodeUnauthorized, Message: "unauthorized"}
// 		var expectedError *UnsafeError
// 		w := New(log, tempDir, &Mirror{unauthorized})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: relatedImages}, opts)
// 		if err == nil {
// 			t.Fatal("should not pass")
// 		}
// 		assert.ErrorAs(t, err, &expectedError)
// 		assert.Equal(t, 0, len(copiedImages.AllImages))
// 	})

// 	t.Run("Testing Worker : manifest unknown for release: should  fail fast", func(t *testing.T) {
// 		opts.Function = "delete"
// 		errorCodeManifestUnknown := errcode.Error{
// 			Code: errcode.ErrorCode(errcode.ErrorCodeManifestUnknown),
// 		}
// 		var expectedError *UnsafeError
// 		w := New(log, tempDir, &Mirror{errorCodeManifestUnknown})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: relatedImages}, opts)
// 		if err == nil {
// 			t.Fatal("should not pass")
// 		}
// 		assert.ErrorAs(t, err, &expectedError)
// 		assert.Equal(t, 0, len(copiedImages.AllImages))
// 	})

// 	t.Run("Testing Worker : manifest unknown for operator image: should  fail safe", func(t *testing.T) {
// 		opImages := []v2alpha1.CopyImageSchema{
// 			{Source: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testa", Type: v2alpha1.TypeOperatorRelatedImage},
// 		}
// 		opts.Function = "delete"
// 		errorCodeManifestUnknown := errcode.Error{
// 			Code: errcode.ErrorCode(errcode.ErrorCodeManifestUnknown),
// 		}
// 		var expectedError *SafeError
// 		w := New(log, tempDir, &Mirror{errorCodeManifestUnknown})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: opImages}, opts)
// 		if err == nil {
// 			t.Fatal("should not pass")
// 		}
// 		assert.ErrorAs(t, err, &expectedError)
// 		assert.Equal(t, len(opImages), len(copiedImages.AllImages))
// 	})

// 	t.Run("Testing Worker : registry connection refused for additional image: should fail safe", func(t *testing.T) {
// 		addImages := []v2alpha1.CopyImageSchema{
// 			{Source: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Origin: "docker://registry/name/namespace/sometestimage-a@sha256:f30638f60452062aba36a26ee6c036feead2f03b28f2c47f2b0a991e41baebea", Destination: "oci:testa", Type: v2alpha1.TypeGeneric},
// 		}
// 		opts.Function = "copy"
// 		refused := syscall.ECONNREFUSED
// 		var expectedError *SafeError
// 		w := New(log, tempDir, &Mirror{refused})
// 		copiedImages, err := w.Worker(context.Background(), v2alpha1.CollectorSchema{AllImages: addImages}, opts)
// 		if err == nil {
// 			t.Fatal("should not pass")
// 		}
// 		assert.ErrorAs(t, err, &expectedError)
// 		assert.Equal(t, len(addImages), len(copiedImages.AllImages))
// 	})
// }

// // mocks

// type Mirror struct {
// 	ForceError error
// }

// func (o Mirror) Run(ctx context.Context, src, dest string, mode mirror.Mode, opts *mirror.CopyOptions) error {
// 	if o.ForceError != nil {
// 		return o.ForceError
// 	}
// 	return nil
// }

// func (o Mirror) Check(ctx context.Context, image string, opts *mirror.CopyOptions, asCopySrc bool) (bool, error) {
// 	if o.ForceError != nil {
// 		return false, o.ForceError
// 	}
// 	return true, nil
// }
