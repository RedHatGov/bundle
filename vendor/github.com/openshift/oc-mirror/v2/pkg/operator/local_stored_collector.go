package operator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	digest "github.com/opencontainers/go-digest"

	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha2"
	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha3"
	"github.com/openshift/oc-mirror/v2/pkg/image"
	clog "github.com/openshift/oc-mirror/v2/pkg/log"
	"github.com/openshift/oc-mirror/v2/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/pkg/mirror"
	"github.com/otiai10/copy"
)

const (
	hashTruncLen int = 12
)

type LocalStorageCollector struct {
	Log              clog.PluggableLoggerInterface
	LogsDir          string
	Mirror           mirror.MirrorInterface
	Manifest         manifest.ManifestInterface
	Config           v1alpha2.ImageSetConfiguration
	Opts             mirror.CopyOptions
	LocalStorageFQDN string
}

// OperatorImageCollector - this looks into the operator index image
// taking into account the mode we are in (mirrorToDisk, diskToMirror)
// the image is downloaded (oci format) and the index.json is inspected
// once unmarshalled, the links to manifests are inspected
func (o *LocalStorageCollector) OperatorImageCollector(ctx context.Context) ([]v1alpha3.CopyImageSchema, error) {

	var (
		allImages      []v1alpha3.CopyImageSchema
		label          string
		dir            string
		imageReference string
		imageName      string
	)
	relatedImages := make(map[string][]v1alpha3.RelatedImage)

	f, err := os.Create(filepath.Join(o.LogsDir, logsFile))
	if err != nil {
		o.Log.Error(errMsg, err)
	}
	writer := bufio.NewWriter(f)
	defer f.Close()
	for _, op := range o.Config.Mirror.Operators {
		// download the operator index image
		o.Log.Info("copying operator image %v", op.Catalog)
		hld := strings.Split(op.Catalog, "/")
		imageIndexDir := strings.Replace(hld[len(hld)-1], ":", "/", -1)
		cacheDir := strings.Join([]string{o.Opts.Global.WorkingDir, operatorImageExtractDir, imageIndexDir}, "/")
		dir = strings.Join([]string{o.Opts.Global.WorkingDir, operatorImageDir, imageIndexDir}, "/")

		// CLID-27 ensure we pick up oci:// (on disk) catalogs
		imgSpec, err := image.ParseRef(op.Catalog)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}
		if imgSpec.Transport == ociProtocol {
			// delete the existing directory and untarred cache contents
			os.RemoveAll(dir)
			os.RemoveAll(cacheDir)
			// copy all contents to the working dir
			err := copy.Copy(imgSpec.PathComponent, dir)
			if err != nil {
				return []v1alpha3.CopyImageSchema{}, err
			}
			// check if we have TargetTag set
			imageReference = imgSpec.ReferenceWithTransport
			name := path.Base(imgSpec.Reference)
			if len(op.TargetTag) > 0 {
				// add it as an annotation
				imageName = name + annotation + op.TargetTag
			} else {
				imageName = name
			}
		} else {
			if _, err := os.Stat(cacheDir); errors.Is(err, os.ErrNotExist) {
				err := os.MkdirAll(dir, 0755)
				if err != nil {
					return []v1alpha3.CopyImageSchema{}, err
				}
				src := dockerProtocol + op.Catalog
				dest := ociProtocolTrimmed + dir
				err = o.Mirror.Run(ctx, src, dest, "copy", &o.Opts, *writer)
				writer.Flush()
				if err != nil {
					o.Log.Error(errMsg, err)
				}
				// read the logs
				f, _ := os.ReadFile(logsFile)
				lines := strings.Split(string(f), "\n")
				for _, s := range lines {
					if len(s) > 0 {
						o.Log.Debug("%s ", strings.ToLower(s))
					}
				}
			}
			imageReference = op.Catalog
			imageName = "index"
		}

		// it's in oci format so we can go directly to the index.json file
		oci, err := o.Manifest.GetImageIndex(dir)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		//read the link to the manifest
		if len(oci.Manifests) == 0 {
			return []v1alpha3.CopyImageSchema{}, fmt.Errorf("[OperatorImageCollector] no manifests found for %s ", op.Catalog)
		}
		validDigest, err := digest.Parse(oci.Manifests[0].Digest)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, fmt.Errorf("[OperatorImageCollector] the digests seem to be incorrect for %s: %v ", op.Catalog, err)
		}

		manifest := validDigest.Encoded()
		o.Log.Info("manifest %v", manifest)
		// read the operator image manifest
		manifestDir := filepath.Join(dir, blobsDir, manifest)
		oci, err = o.Manifest.GetImageManifest(manifestDir)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		// read the config digest to get the detailed manifest
		// looking for the lable to search for a specific folder
		configDigest, err := digest.Parse(oci.Config.Digest)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, fmt.Errorf("[OperatorImageCollector] the digests seem to be incorrect for %s: %v ", op.Catalog, err)
		}
		catalogDir := filepath.Join(dir, blobsDir, configDigest.Encoded())
		ocs, err := o.Manifest.GetOperatorConfig(catalogDir)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		label = ocs.Config.Labels.OperatorsOperatorframeworkIoIndexConfigsV1
		o.Log.Info("label %s", label)

		// untar all the blobs for the operator
		// if the layer with "label (from previous step) is found to a specific folder"
		fromDir := strings.Join([]string{dir, blobsDir}, "/")
		err = o.Manifest.ExtractLayersOCI(fromDir, cacheDir, label, oci)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		operatorCatalog, err := o.Manifest.GetCatalog(filepath.Join(cacheDir, label))
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		relatedImages, err = o.Manifest.GetRelatedImagesFromCatalog(operatorCatalog, op)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}

		// we add the manifest digest if we don't have the TargetTag as annotation
		if !strings.Contains(imageName, annotation) {
			imageName = imageName + annotation + validDigest.String()
		}
		relatedImages["index"] = []v1alpha3.RelatedImage{
			{
				Name:  imageName,
				Image: imageReference,
				Type:  v1alpha2.TypeOperatorCatalog,
			},
		}
	}

	o.Log.Info("related images length %d ", len(relatedImages))
	var count = 0
	for _, v := range relatedImages {
		count = count + len(v)
	}
	o.Log.Info("images to copy (before duplicates) %d ", count)

	// check the mode
	if o.Opts.IsMirrorToDisk() || o.Opts.IsPrepare() {
		allImages, err = o.prepareM2DCopyBatch(o.Log, dir, relatedImages)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}
	}

	if o.Opts.IsDiskToMirror() {
		allImages, err = o.prepareD2MCopyBatch(o.Log, dir, relatedImages)
		if err != nil {
			return []v1alpha3.CopyImageSchema{}, err
		}
	}
	return allImages, nil
}

func (o LocalStorageCollector) prepareD2MCopyBatch(log clog.PluggableLoggerInterface, dir string, images map[string][]v1alpha3.RelatedImage) ([]v1alpha3.CopyImageSchema, error) {
	var result []v1alpha3.CopyImageSchema
	for _, relatedImgs := range images {
		for _, img := range relatedImgs {
			var src string
			var dest string
			if !strings.HasPrefix(img.Image, ociProtocol) {

				imgSpec, err := image.ParseRef(img.Image)
				if err != nil {
					o.Log.Error("%s", err.Error())
					return nil, err
				}

				if imgSpec.IsImageByDigest() {
					src = dockerProtocol + strings.Join([]string{o.LocalStorageFQDN, imgSpec.PathComponent + ":" + imgSpec.Digest[:hashTruncLen]}, "/")
					dest = strings.Join([]string{o.Opts.Destination, imgSpec.PathComponent + ":" + imgSpec.Digest[:hashTruncLen]}, "/")
				} else {
					src = dockerProtocol + strings.Join([]string{o.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
					dest = strings.Join([]string{o.Opts.Destination, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				}
			} else {
				src = img.Image
				transportAndPath := strings.Split(img.Image, "://")
				if len(transportAndPath) == 2 {
					dest = dockerProtocol + strings.Join([]string{o.Opts.Destination, transportAndPath[1]}, "/")
				} else { // no transport prefix
					dest = dockerProtocol + strings.Join([]string{o.Opts.Destination, img.Image}, "/")
				}
			}

			if src == "" || dest == "" {
				return result, fmt.Errorf("unable to determine src %s or dst %s for %s", src, dest, img.Image)
			}

			o.Log.Debug("source %s", src)
			o.Log.Debug("destination %s", dest)
			result = append(result, v1alpha3.CopyImageSchema{Origin: img.Image, Source: src, Destination: dest, Type: img.Type})
		}
	}
	return result, nil
}

func (o LocalStorageCollector) prepareM2DCopyBatch(log clog.PluggableLoggerInterface, dir string, images map[string][]v1alpha3.RelatedImage) ([]v1alpha3.CopyImageSchema, error) {
	var result []v1alpha3.CopyImageSchema
	for _, relatedImgs := range images {
		for _, img := range relatedImgs {
			var src string
			var dest string
			imgSpec, err := image.ParseRef(img.Image)
			if err != nil {
				return nil, err
			}
			if imgSpec.Transport == ociProtocol {
				src = imgSpec.ReferenceWithTransport
				// as we have annotated the image name with a tag, we can do some informative tagging
				// remove our annotation add the TargetTag or digest
				name := strings.Split(img.Name, annotation)
				var nameAndTag string
				if len(name) < 2 {
					return nil, fmt.Errorf("could not find '-annotation-' in image name for oci fbc catalog")
				}
				if strings.Contains(name[1], sha256) {
					tag := strings.Split(name[1], sha256)[1]
					nameAndTag = name[0] + ":" + tag[:hashTruncLen]
				} else {
					nameAndTag = name[0] + ":" + name[1]
				}
				dest = dockerProtocol + strings.Join([]string{o.LocalStorageFQDN, nameAndTag}, "/")
			} else {
				src = imgSpec.ReferenceWithTransport
				if imgSpec.IsImageByDigest() {
					dest = dockerProtocol + strings.Join([]string{o.LocalStorageFQDN, imgSpec.PathComponent + ":" + imgSpec.Digest[:hashTruncLen]}, "/")
				} else {
					dest = dockerProtocol + strings.Join([]string{o.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				}
			}

			o.Log.Debug("source %s", src)
			o.Log.Debug("destination %s", dest)
			result = append(result, v1alpha3.CopyImageSchema{Source: src, Destination: dest, Origin: src, Type: img.Type})

		}
	}
	return result, nil
}
