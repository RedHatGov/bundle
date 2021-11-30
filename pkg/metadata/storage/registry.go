package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"github.com/sirupsen/logrus"

	"github.com/openshift/oc-mirror/pkg/config/v1alpha1"
)

var _ Backend = &registryBackend{}

type registryBackend struct {
	// Since image contents are represented locally as directories,
	// use the local dir backend as the underlying Backend.
	*localDirBackend
	// Image to use when pushing and pulling
	src string
	// Registry client options
	insecure bool
	ctx      context.Context
}

func NewRegistryBackend(ctx context.Context, cfg *v1alpha1.RegistryConfig, dir string) (Backend, error) {
	r := registryBackend{}
	r.src = cfg.ImageURL
	r.insecure = cfg.SkipTLS
	r.ctx = ctx

	if r.localDirBackend == nil {
		// Create the local dir backend for local r/w.
		lb, err := NewLocalBackend(dir)
		if err != nil {
			return nil, fmt.Errorf("error creating local backend for registry: %w", err)
		}
		r.localDirBackend = lb.(*localDirBackend)
	}

	return &r, nil
}

// ReadMetadata unpacks the metadata image and read it from disk
func (r *registryBackend) ReadMetadata(ctx context.Context, meta *v1alpha1.Metadata, path string) error {
	logrus.Debugf("Checking for existing metadata image at %s", r.src)
	// Check if image exists
	if err := r.exists(); err != nil {
		return err
	}

	// Get metadata from image
	err := r.unpack(r.localDirBackend.dir)
	if err != nil {
		return fmt.Errorf("error pulling image %q with metadata: %v", r.src, err)
	}

	// adjust perms, unpack leaves the file user-writable only
	fpath := filepath.Join(r.localDirBackend.dir, path)
	err = os.Chmod(fpath, 0600)
	if err != nil {
		return err
	}

	return r.localDirBackend.ReadMetadata(ctx, meta, path)
}

// WriteMetadata writes the provided metadata to disk anf registry.
func (r *registryBackend) WriteMetadata(ctx context.Context, meta *v1alpha1.Metadata, path string) error {
	return r.WriteObject(ctx, path, meta)
}

// ReadObject reads the provided object from disk.
// In this implementation, key is a file path.
func (r *registryBackend) ReadObject(ctx context.Context, fpath string, obj interface{}) error {
	return r.localDirBackend.ReadObject(ctx, fpath, obj)
}

// WriteObject writes the provided object to disk and registry.
// In this implementation, key is a file path.
func (r *registryBackend) WriteObject(ctx context.Context, fpath string, obj interface{}) (err error) {
	var data []byte
	switch v := obj.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	case io.Reader:
		data, err = io.ReadAll(v)
	default:
		data, err = json.Marshal(obj)
	}
	if err != nil {
		return err
	}

	// Write metadata to disk for packing into archive
	if err := r.localDirBackend.WriteObject(ctx, fpath, obj); err != nil {
		return err
	}
	logrus.Debugf("Pushing metadata to registry at %s", r.src)
	return r.pushImage(data, fpath)
}

// GetWriter returns an os.File as a writer.
// In this implementation, key is a file path.
func (r *registryBackend) GetWriter(ctx context.Context, fpath string) (io.Writer, error) {
	return r.localDirBackend.GetWriter(ctx, fpath)
}

// CheckConfig will return an error if the StorageConfig
// is not a registry
func (r *registryBackend) CheckConfig(storage v1alpha1.StorageConfig) error {
	if storage.Registry == nil {
		return fmt.Errorf("not registry backend")
	}
	return nil
}

// pushImage will push a v1.Image with provided contents
func (r *registryBackend) pushImage(data []byte, fpath string) error {
	var options []crane.Option

	rt := r.createRT()
	options = append(options, crane.WithTransport(rt))
	options = append(options, crane.WithContext(r.ctx))

	contents := map[string][]byte{
		fpath: data,
	}
	i, _ := crane.Image(contents)
	return crane.Push(i, r.src, options...)
}

func (r *registryBackend) createRegistry() (*containerdregistry.Registry, error) {
	cacheDir, err := os.MkdirTemp("", "imageset-catalog-registry-")
	if err != nil {
		return nil, err
	}

	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)
	nullLogger := logrus.NewEntry(logger)

	return containerdregistry.NewRegistry(
		containerdregistry.WithCacheDir(cacheDir),
		containerdregistry.SkipTLS(r.insecure),
		// The containerd registry impl is somewhat verbose, even on the happy path,
		// so discard all logger logs}. Any important failures will be returned from
		// registry methods and eventually logged as fatal errors.
		containerdregistry.WithLog(nullLogger),
	)
}

func (r *registryBackend) unpack(path string) error {
	reg, err := r.createRegistry()
	if err != nil {
		return fmt.Errorf("error creating container registry: %v", err)
	}
	defer reg.Destroy()
	ref := image.SimpleReference(r.src)
	if err := reg.Pull(r.ctx, ref); err != nil {
		return err
	}
	_, err = reg.Labels(r.ctx, ref)
	if err != nil {
		return err
	}
	return reg.Unpack(r.ctx, ref, path)
}

// exists checks if the image exists
func (r *registryBackend) exists() error {
	_, err := crane.Manifest(r.src, r.getOpts()...)
	var terr *transport.Error
	switch {
	case err == nil:
		return nil
	case err != nil && errors.As(err, &terr):
		return ErrMetadataNotExist
	default:
		return err
	}
}

func (r *registryBackend) createRT() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: r.insecure}
	return transport
}

// TODO: Get default auth will need to update if user
// can specify custom locations
func (r *registryBackend) getOpts() (options []crane.Option) {
	return append(
		options,
		crane.WithAuthFromKeychain(authn.DefaultKeychain),
		crane.WithContext(r.ctx),
		crane.WithTransport(r.createRT()),
	)
}
