package mirror

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/openshift/library-go/pkg/image/reference"
	"github.com/openshift/oc/pkg/cli/image/imagesource"
	"github.com/operator-framework/operator-registry/pkg/containertools"
	"github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"k8s.io/klog/v2"

	"github.com/openshift/oc-mirror/pkg/api/v1alpha2"
	"github.com/openshift/oc-mirror/pkg/config"
	"github.com/openshift/oc-mirror/pkg/image"
	"github.com/openshift/oc-mirror/pkg/image/builder"
)

const (
	opmCachePrefix  = "/tmp/cache"
	opmBinarySuffix = "opm"
	opmBinaryPrefix = "usr/bin/registry/opm"
	opmBinaryDir    = "usr/bin/registry"
)

// unpackCatalog will unpack file-based catalogs if they exists
func (o *MirrorOptions) unpackCatalog(dstDir string, filesInArchive map[string]string) (bool, error) {
	var found bool
	if err := unpack(config.CatalogsDir, dstDir, filesInArchive); err != nil {
		nferr := &ErrArchiveFileNotFound{}
		if errors.As(err, &nferr) || errors.Is(err, os.ErrNotExist) {
			klog.V(2).Info("No catalogs found in archive, skipping catalog rebuild")
			return found, nil
		}
		return found, err
	}
	found = true
	return found, nil
}

/*
rebuildCatalogs will modify an OCI catalog in <some path>/src/catalogs/<repoPath>/layout with
the index.json files found in <some path>/src/catalogs/<repoPath>/index/index.json

# Arguments

• ctx: cancellation context

• dstDir: the path to where the config.SourceDir resides

# Returns

• image.TypedImageMapping: the source/destination mapping for the catalog

• error: non-nil if error occurs, nil otherwise
*/
func (o *MirrorOptions) rebuildCatalogs(ctx context.Context, dstDir string) (image.TypedImageMapping, error) {
	refs := image.TypedImageMapping{}
	var err error

	mirrorRef := imagesource.TypedImageReference{Type: imagesource.DestinationRegistry}
	mirrorRef.Ref, err = reference.Parse(o.ToMirror)
	if err != nil {
		return nil, err
	}

	dstDir = filepath.Clean(dstDir)
	catalogsByImage := map[image.TypedImage]string{}
	if err := filepath.Walk(dstDir, func(fpath string, info fs.FileInfo, err error) error {

		// Skip the layouts dir because we only need
		// to process the parent directory one time
		if filepath.Base(fpath) == config.LayoutsDir {
			return filepath.SkipDir
		}

		if err != nil || info == nil {
			return err
		}

		// From the index path determine the artifacts (index and layout) directory.
		// Using that path to determine the corresponding catalog image for processing.
		slashPath := filepath.ToSlash(fpath)
		if base := path.Base(slashPath); base == "index.json" {
			// remove the index.json from the path
			// results in <some path>/src/catalogs/<repoPath>/index
			slashPath = path.Dir(slashPath)
			// remove the index folder from the path
			// results in <some path>/src/catalogs/<repoPath>
			slashPath = strings.TrimSuffix(slashPath, config.IndexDir)

			// remove the <some path>/src/catalogs from the path to arrive at <repoPath>
			repoPath := strings.TrimPrefix(slashPath, fmt.Sprintf("%s/%s/", dstDir, config.CatalogsDir))
			// get the repo namespace and id (where ID is a SHA or tag)
			// example: foo.com/foo/bar/<id>
			regRepoNs, id := path.Split(path.Dir(repoPath))
			regRepoNs = path.Clean(regRepoNs)
			// reconstitute the path into a valid docker ref
			var img string
			if strings.Contains(id, ":") {
				// Digest.
				img = fmt.Sprintf("%s@%s", regRepoNs, id)
			} else {
				// Tag.
				img = fmt.Sprintf("%s:%s", regRepoNs, id)
			}
			ctlgRef := image.TypedImage{}
			ctlgRef.Type = imagesource.DestinationRegistry
			sourceRef, err := image.ParseReference(img)
			// since we can't really tell if the "img" reference originated from an actual docker
			// reference or from an OCI file path that approximates a docker reference, ParseReference
			// might not lowercase the name and namespace values which is required by the
			// docker reference spec (see https://github.com/distribution/distribution/blob/main/reference/reference.go).
			// Therefore we lower case name and namespace here to make sure it's done.
			sourceRef.Ref.Name = strings.ToLower(sourceRef.Ref.Name)
			sourceRef.Ref.Namespace = strings.ToLower(sourceRef.Ref.Namespace)

			if err != nil {
				return fmt.Errorf("error parsing index dir path %q as image %q: %v", fpath, img, err)
			}
			ctlgRef.Ref = sourceRef.Ref
			// Update registry so the existing catalog image can be pulled.
			ctlgRef.Ref.Registry = mirrorRef.Ref.Registry
			ctlgRef.Ref.Namespace = path.Join(o.UserNamespace, ctlgRef.Ref.Namespace)
			ctlgRef = ctlgRef.SetDefaults()
			// Unset the ID when passing to the image builder.
			// Tags are needed here since the digest will be recalculated.
			ctlgRef.Ref.ID = ""

			catalogsByImage[ctlgRef] = slashPath

			// Add to mapping for ICSP generation
			refs.Add(sourceRef, ctlgRef.TypedImageReference, v1alpha2.TypeOperatorCatalog)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// update the catalogs in the OCI layout directory and push them to their destination
	if err := o.processCatalogRefs(ctx, catalogsByImage); err != nil {
		return nil, err
	}

	// use the resolver to obtain the digests of the newly pushed images
	resolver, err := containerdregistry.NewResolver("", o.DestSkipTLS, o.DestPlainHTTP, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating image resolver: %v", err)
	}

	// Resolve the image's digest for ICSP creation.
	for source, dest := range refs {
		_, desc, err := resolver.Resolve(ctx, dest.Ref.Exact())
		if err != nil {
			return nil, fmt.Errorf("error retrieving digest for catalog image %q: %v", dest.Ref.Exact(), err)
		}
		dest.Ref.ID = desc.Digest.String()
		refs[source] = dest
	}

	return refs, nil
}

/*
processCatalogRefs uses the image builder to update a given image using the data provided in catalogRefs.

# Arguments

• ctx: cancellation context

• catalogsByImage: key is catalog destination reference, value is <some path>/src/catalogs/<repoPath>

# Returns

• error: non-nil if error occurs, nil otherwise
*/
func (o *MirrorOptions) processCatalogRefs(ctx context.Context, catalogsByImage map[image.TypedImage]string) error {
	for ctlgRef, artifactDir := range catalogsByImage {
		// Always build the catalog image with the new declarative config catalog
		// using the original catalog as the base image
		var layoutPath layout.Path
		refExact := ctlgRef.Ref.Exact()

		var destInsecure bool
		if o.DestPlainHTTP || o.DestSkipTLS {
			destInsecure = true
		}

		// Check push permissions before trying to resolve for Quay compatibility
		nameOpts := getNameOpts(destInsecure)
		remoteOpts := getRemoteOpts(ctx, destInsecure)
		imgBuilder := builder.NewImageBuilder(nameOpts, remoteOpts)

		klog.Infof("Rendering catalog image %q with file-based catalog ", refExact)

		configLayerToAdd, err := builder.LayerFromPath("/configs", filepath.Join(artifactDir, config.IndexDir, "index.json"))
		if err != nil {
			return fmt.Errorf("error creating add layer: %v", err)
		}

		// Since we are defining the FBC as index.json,
		// remove anything that may currently exist
		deletedConfigLayer, err := deleteLayer("/.wh.configs")
		if err != nil {
			return fmt.Errorf("error creating deleted layer: %v", err)
		}

		//TODO how do you know which is the cache dir

		//TODO white out layer /tmp
		deletedCacheLayer, err := deleteLayer("/tmp/.wh.cache")
		if err != nil {
			return fmt.Errorf("error creating deleted cache layer: %v", err)
		}

		opmCmdPath, err := findOpmCmd(artifactDir)
		if err != nil {
			return fmt.Errorf("cannot find opm in the extracted catalog %v for %s on %s: %v", ctlgRef, runtime.GOOS, runtime.GOARCH, err)
		}
		//TODO call opm serve /configs –-cache-dir /tmp/cache –-cache-only
		cmd := exec.Command(opmCmdPath, "serve", filepath.Join(artifactDir, config.IndexDir), "--cache-dir", filepath.Join(artifactDir, config.TmpDir), "--cache-only")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("error regenerating the cache for %v: %v", ctlgRef, err)
		}
		//TODO check the tmp folder is not empty
		//TODO create a new tmp layer from reconstructed cache
		cacheLayerToAdd, err := builder.LayerFromPath("/tmp/cache", filepath.Join(artifactDir, config.TmpDir))
		if err != nil {
			return fmt.Errorf("error creating add layer: %v", err)
		}

		// Deleted layers must be added first in the slice
		// so that the /configs and /tmp directories are deleted
		// and then added back from the layers rebuilt from the new FBC.
		layers := []v1.Layer{deletedConfigLayer, deletedCacheLayer, configLayerToAdd, cacheLayerToAdd}

		layoutDir := filepath.Join(artifactDir, config.LayoutsDir)
		layoutPath, err = imgBuilder.CreateLayout("", layoutDir)
		if err != nil {
			return fmt.Errorf("error creating OCI layout: %v", err)
		}

		update := func(cfg *v1.ConfigFile) {
			labels := map[string]string{
				containertools.ConfigsLocationLabel: "/configs",
			}
			cfg.Config.Labels = labels
		}
		if err := imgBuilder.Run(ctx, refExact, layoutPath, update, layers...); err != nil {
			return fmt.Errorf("error building catalog layers: %v", err)
		}
	}
	return nil
}

func findOpmCmd(artifactDir string) (string, error) {
	//TODO guess the opmCmdPath
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("error finding current working directory while preparing to run opm to regenerate cache: %v", err)
	}
	runningOS := runtime.GOOS
	runningArch := runtime.GOARCH
	opmBin := "opm"

	if runningOS != "linux" {
		opmBin = strings.Join([]string{runningOS, runningArch, opmBin}, "-")
	}
	opmCmdPath := filepath.Join(wd, artifactDir, config.OpmBinDir, opmBinaryDir, opmBin)
	_, err = os.Stat(opmCmdPath)
	if err != nil {
		return "", fmt.Errorf("error finding the extracted opm binary %s while preparing to run opm to regenerate cache: %v", opmCmdPath, err)
	}
	err = os.Chmod(opmCmdPath, 0744)
	if err != nil {
		return "", fmt.Errorf("error changing permissions to the extracted opm binary while preparing to run opm to regenerate cache: %v", err)
	}
	return opmCmdPath, nil
}

func extractOPMBinary(srcRef image.TypedImageReference, outDir string) error {
	var img v1.Image
	var err error
	refExact := srcRef.Ref.Exact()
	if srcRef.OCIFBCPath == "" {

		img, err = crane.Pull(refExact)
		if err != nil {
			return fmt.Errorf("unable to pull image from %s: %v", refExact, err)
		}
	} else {

		// obtain the path to where the OCI image reference resides
		layoutPath := layout.Path(v1alpha2.TrimProtocol(srcRef.OCIFBCPath))

		// get its index.json and obtain its manifest
		rootIndex, err := layoutPath.ImageIndex()
		if err != nil {
			return err
		}
		rootIndexManifest, err := rootIndex.IndexManifest()
		if err != nil {
			return err
		}

		// attempt to find the first image reference in the layout...
		// for a manifest list only search one level deep.

	loop:
		for _, descriptor := range rootIndexManifest.Manifests {

			if descriptor.MediaType.IsIndex() {
				// follow the descriptor using its digest to get the referenced index and its manifest
				childIndex, err := rootIndex.ImageIndex(descriptor.Digest)
				if err != nil {
					return err
				}
				childIndexManifest, err := childIndex.IndexManifest()
				if err != nil {
					return err
				}

				// at this point, find the first image and store it for later if possible
				//TODO extract the child index that corresponds to this machine's architecture
				for _, childDescriptor := range childIndexManifest.Manifests {
					if childDescriptor.MediaType.IsImage() && childDescriptor.Platform.Architecture == runtime.GOARCH && childDescriptor.Platform.OS == runtime.GOOS {
						img, err = childIndex.Image(childDescriptor.Digest)
						if err != nil {
							return err
						}
						// no further processing necessary
						break loop
					}
				}

			} else if descriptor.MediaType.IsImage() {
				// this is a direct reference to an image, so just store it for later
				img, err = rootIndex.Image(descriptor.Digest)
				if err != nil {
					return err
				}
				// no further processing necessary
				break loop
			}
		}
	}
	// if we get here and no image was found bail out
	if img == nil {
		return fmt.Errorf("unable to obtain image for %v", srcRef)
	}
	tr := tar.NewReader(mutate.Extract(img))
	for {
		header, err := tr.Next()

		// break the infinite loop when EOF
		if errors.Is(err, io.EOF) {
			break
		}

		// skip the file if it is a directory or not in the bin dir
		if !strings.HasSuffix(header.Name, opmBinarySuffix) || header.FileInfo().IsDir() {
			continue
		}

		var buf bytes.Buffer
		_, err = buf.ReadFrom(tr)
		if err != nil {
			return err
		}

		targetFileName := filepath.Join(outDir, header.Name)
		bytes := buf.Bytes()

		baseDir := filepath.Dir(targetFileName)
		err = os.MkdirAll(baseDir, 0755)
		if err != nil {
			return err
		}

		f, err := os.Create(targetFileName)
		if err == nil {
			defer f.Close()
		} else {
			return err
		}

		_, err = f.Write(bytes)
		if err != nil {
			return err
		}
	}
	//TODO use a constant cmd/oc-mirror/oc-mirror-workspace/src/catalogs/registry.redhat.io/redhat-operator-index/redhat-operator-index/v4.13/bin/usr/bin/registry/opm
	returnPath := filepath.Join(outDir, opmBinaryPrefix)
	// check for the folder (it should exist if we found something)
	_, err = os.Stat(returnPath)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("opm binary not found after extracting %q within image", opmBinaryPrefix)
	}
	return nil
}

func deleteLayer(old string) (v1.Layer, error) {
	deleteMap := map[string][]byte{}
	deleteMap[old] = []byte{}
	return crane.Layer(deleteMap)
}
