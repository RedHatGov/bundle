package operator

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/containers/image/v5/types"
	"github.com/opencontainers/go-digest"
	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
	"github.com/openshift/oc-mirror/v2/internal/pkg/image"
	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/otiai10/copy"
)

type FilterCollector struct {
	OperatorCollector
}

// OperatorImageCollector - this looks into the operator index image
// taking into account the mode we are in (mirrorToDisk, diskToMirror)
// the image is downloaded (oci format) and the index.json is inspected
// once unmarshalled, the links to manifests are inspected
func (o *FilterCollector) OperatorImageCollector(ctx context.Context) (v2alpha1.CollectorSchema, error) {

	var (
		allImages       []v2alpha1.CopyImageSchema
		label           string
		catalogImageDir string
		catalogName     string
		rebuiltTag      string
	)
	o.Log.Debug(collectorPrefix+"setting copy option o.Opts.MultiArch=%s when collecting operator images", o.Opts.MultiArch)

	relatedImages := make(map[string][]v2alpha1.RelatedImage)
	collectorSchema := v2alpha1.CollectorSchema{}
	copyImageSchemaMap := &v2alpha1.CopyImageSchemaMap{OperatorsByImage: make(map[string]map[string]struct{}), BundlesByImage: make(map[string]map[string]string)}

	for _, op := range o.Config.Mirror.Operators {
		var catalogImage string
		// download the operator index image
		o.Log.Debug(collectorPrefix+"copying operator image %s", op.Catalog)

		// CLID-47 double check that targetCatalog is valid
		if op.TargetCatalog != "" && !v2alpha1.IsValidPathComponent(op.TargetCatalog) {
			o.Log.Error(collectorPrefix+"invalid targetCatalog %s", op.TargetCatalog)
			return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"invalid targetCatalog %s", op.TargetCatalog)
		}
		// CLID-27 ensure we pick up oci:// (on disk) catalogs
		imgSpec, err := image.ParseRef(op.Catalog)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return v2alpha1.CollectorSchema{}, err
		}
		//OCPBUGS-36214: For diskToMirror (and delete), access to the source registry is not guaranteed
		catalogDigest := ""
		if o.Opts.Mode == mirror.DiskToMirror || o.Opts.Mode == string(mirror.DeleteMode) {
			d, err := o.catalogDigest(ctx, op)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}
			catalogDigest = d
		} else {
			sourceCtx, err := o.Opts.SrcImage.NewSystemContext()
			if err != nil {
				return v2alpha1.CollectorSchema{}, err
			}
			d, err := o.Manifest.GetDigest(ctx, sourceCtx, imgSpec.ReferenceWithTransport)
			// OCPBUGS-36548 (manifest unknown)
			if err != nil {
				o.Log.Warn(collectorPrefix+"catalog %s : SKIPPING", err.Error())
				continue
			}
			catalogDigest = d
		}

		imageIndex := filepath.Join(imgSpec.ComponentName(), catalogDigest)
		imageIndexDir := filepath.Join(o.Opts.Global.WorkingDir, operatorCatalogsDir, imageIndex)
		configsDir := filepath.Join(imageIndexDir, operatorCatalogConfigDir)
		catalogImageDir = filepath.Join(imageIndexDir, operatorCatalogImageDir)
		filteredCatalogsDir := filepath.Join(imageIndexDir, operatorCatalogFilteredDir)

		err = createFolders([]string{configsDir, catalogImageDir, filteredCatalogsDir})
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return v2alpha1.CollectorSchema{}, err
		}

		var filteredDC *declcfg.DeclarativeConfig
		var isAlreadyFiltered bool

		filterDigest, err := digestOfFilter(op)
		if err != nil {
			return v2alpha1.CollectorSchema{}, err
		}
		rebuiltTag = filterDigest
		var srcFilteredCatalog string
		filterPath := filepath.Join(filteredCatalogsDir, filterDigest, "digest")
		filteredImageDigest, err := os.ReadFile(filterPath)
		if err == nil && len(filterDigest) > 0 {
			srcFilteredCatalog, err = o.cachedCatalog(op, filterDigest)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}
			isAlreadyFiltered = o.isAlreadyFiltered(ctx, srcFilteredCatalog, string(filteredImageDigest))
		}

		if isAlreadyFiltered {
			filterConfigDir := filepath.Join(filteredCatalogsDir, filterDigest, operatorCatalogConfigDir)
			filteredDC, err = o.ctlgHandler.getDeclarativeConfig(filterConfigDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}
			if len(op.TargetCatalog) > 0 {
				catalogName = op.TargetCatalog
			} else {
				catalogName = path.Base(imgSpec.Reference)
			}
			if imgSpec.Transport == ociProtocol {
				catalogImageDir, err := filepath.Abs(catalogImageDir)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return v2alpha1.CollectorSchema{}, err
				}
				// ensure correct oci format and directory lookup
				catalogImage = ociProtocol + catalogImageDir
			} else {
				catalogImage = op.Catalog
			}
			catalogDigest = string(filteredImageDigest)
			if collectorSchema.CatalogToFBCMap == nil {
				collectorSchema.CatalogToFBCMap = make(map[string]v2alpha1.CatalogFilterResult)
			}
			result := v2alpha1.CatalogFilterResult{
				OperatorFilter:     op,
				FilteredConfigPath: filterConfigDir,
			}
			collectorSchema.CatalogToFBCMap[imgSpec.ReferenceWithTransport] = result

		} else {
			if imgSpec.Transport == ociProtocol {
				if _, err := os.Stat(filepath.Join(catalogImageDir, "index.json")); errors.Is(err, os.ErrNotExist) {
					// delete the existing directory and untarred cache contents
					os.RemoveAll(catalogImageDir)
					os.RemoveAll(configsDir)
					// copy all contents to the working dir
					err := copy.Copy(imgSpec.PathComponent, catalogImageDir)
					if err != nil {
						o.Log.Error(errMsg, err.Error())
						return v2alpha1.CollectorSchema{}, err
					}
				}

				if len(op.TargetCatalog) > 0 {
					catalogName = op.TargetCatalog
				} else {
					catalogName = path.Base(imgSpec.Reference)
				}
			} else {
				src := dockerProtocol + op.Catalog
				dest := ociProtocolTrimmed + catalogImageDir

				optsCopy := o.Opts
				optsCopy.Stdout = io.Discard

				err = o.Mirror.Run(ctx, src, dest, "copy", &optsCopy)

				if err != nil {
					o.Log.Error(errMsg, err.Error())
				}
			}

			// it's in oci format so we can go directly to the index.json file
			oci, err := o.Manifest.GetImageIndex(catalogImageDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}

			if isMultiManifestIndex(*oci) && imgSpec.Transport == ociProtocol {
				err = o.Manifest.ConvertIndexToSingleManifest(catalogImageDir, oci)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return v2alpha1.CollectorSchema{}, err
				}

				oci, err = o.Manifest.GetImageIndex(catalogImageDir)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return v2alpha1.CollectorSchema{}, err
				}

				catalogImage = ociProtocol + catalogImageDir
			} else {
				catalogImage = op.Catalog
			}

			if len(oci.Manifests) == 0 {
				o.Log.Error(collectorPrefix+"no manifests found for %s ", op.Catalog)
				return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"no manifests found for %s ", op.Catalog)
			}

			validDigest, err := digest.Parse(oci.Manifests[0].Digest)
			if err != nil {
				o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
				return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
			}

			manifest := validDigest.Encoded()
			o.Log.Debug(collectorPrefix+"manifest %s", manifest)
			// read the operator image manifest
			manifestDir := filepath.Join(catalogImageDir, blobsDir, manifest)
			oci, err = o.Manifest.GetImageManifest(manifestDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}

			// we need to check if oci returns multi manifests
			// (from manifest list) also oci.Config will be nil
			// we are only interested in the first manifest as all
			// architecture "configs" will be exactly the same
			if len(oci.Manifests) > 1 && oci.Config.Size == 0 {
				subDigest, err := digest.Parse(oci.Manifests[0].Digest)
				if err != nil {
					o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
					return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
				}
				manifestDir := filepath.Join(catalogImageDir, blobsDir, subDigest.Encoded())
				oci, err = o.Manifest.GetImageManifest(manifestDir)
				if err != nil {
					o.Log.Error(collectorPrefix+"manifest %s: %s ", op.Catalog, err.Error())
					return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"manifest %s: %s ", op.Catalog, err.Error())
				}
			}

			// read the config digest to get the detailed manifest
			// looking for the lable to search for a specific folder
			configDigest, err := digest.Parse(oci.Config.Digest)
			if err != nil {
				o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
				return v2alpha1.CollectorSchema{}, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
			}
			catalogDir := filepath.Join(catalogImageDir, blobsDir, configDigest.Encoded())
			ocs, err := o.Manifest.GetOperatorConfig(catalogDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return v2alpha1.CollectorSchema{}, err
			}

			label = ocs.Config.Labels.OperatorsOperatorframeworkIoIndexConfigsV1
			o.Log.Debug(collectorPrefix+"label %s", label)

			// untar all the blobs for the operator
			// if the layer with "label (from previous step) is found to a specific folder"
			fromDir := strings.Join([]string{catalogImageDir, blobsDir}, "/")
			err = o.Manifest.ExtractLayersOCI(fromDir, configsDir, label, oci)
			if err != nil {
				return v2alpha1.CollectorSchema{}, err
			}

			originalDC, err := o.ctlgHandler.getDeclarativeConfig(filepath.Join(configsDir, label))
			if err != nil {
				return v2alpha1.CollectorSchema{}, err
			}

			if !isFullCatalog(op) {

				var filteredDigestPath string
				var filterDigest string

				filteredDC, err = filterCatalog(ctx, *originalDC, op)
				if err != nil {
					return v2alpha1.CollectorSchema{}, err
				}

				filterDigest, err = digestOfFilter(op)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return v2alpha1.CollectorSchema{}, err
				}

				if filterDigest != "" {
					filteredDigestPath = filepath.Join(filteredCatalogsDir, filterDigest, operatorCatalogConfigDir)

					err = createFolders([]string{filteredDigestPath})
					if err != nil {
						o.Log.Error(errMsg, err.Error())
						return v2alpha1.CollectorSchema{}, err
					}
				}

				err = saveDeclarativeConfig(*filteredDC, filteredDigestPath)
				if err != nil {
					return v2alpha1.CollectorSchema{}, err
				}

				if collectorSchema.CatalogToFBCMap == nil {
					collectorSchema.CatalogToFBCMap = make(map[string]v2alpha1.CatalogFilterResult)
				}
				result := v2alpha1.CatalogFilterResult{
					OperatorFilter:     op,
					FilteredConfigPath: filteredDigestPath,
				}
				collectorSchema.CatalogToFBCMap[imgSpec.ReferenceWithTransport] = result

			} else {
				rebuiltTag = ""
				filteredDC = originalDC
			}
		}

		ri, err := o.ctlgHandler.getRelatedImagesFromCatalog(filteredDC, copyImageSchemaMap)
		if err != nil {
			return v2alpha1.CollectorSchema{}, err
		}

		maps.Copy(relatedImages, ri)

		var targetTag string
		var targetCatalog string
		if len(op.TargetTag) > 0 {
			targetTag = op.TargetTag
		} else if imgSpec.Transport == ociProtocol {
			// for this case only, img.ParseRef(in its current state)
			// will not be able to determine the digest.
			// this leaves the oci imgSpec with no tag nor digest as it
			// goes to prepareM2DCopyBatch/prepareD2MCopyBath. This is
			// why we set the digest read from manifest in targetTag
			targetTag = "latest"
		}

		if len(op.TargetCatalog) > 0 {
			targetCatalog = op.TargetCatalog

		}

		componentName := imgSpec.ComponentName() + "." + catalogDigest

		relatedImages[componentName] = []v2alpha1.RelatedImage{
			{
				Name:          catalogName,
				Image:         catalogImage,
				Type:          v2alpha1.TypeOperatorCatalog,
				TargetTag:     targetTag,
				TargetCatalog: targetCatalog,
				RebuiltTag:    rebuiltTag,
			},
		}
	}

	o.Log.Debug(collectorPrefix+"related images length %d ", len(relatedImages))
	var count = 0
	if o.Opts.Global.LogLevel == "debug" {
		for _, v := range relatedImages {
			count = count + len(v)
		}
	}
	o.Log.Debug(collectorPrefix+"images to copy (before duplicates) %d ", count)
	var err error
	// check the mode
	switch {
	case o.Opts.IsMirrorToDisk():
		allImages, err = o.prepareM2DCopyBatch(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return v2alpha1.CollectorSchema{}, err
		}
	case o.Opts.IsMirrorToMirror():
		allImages, err = o.dispatchImagesForM2M(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return v2alpha1.CollectorSchema{}, err
		}
	case o.Opts.IsDiskToMirror():
		allImages, err = o.prepareD2MCopyBatch(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return v2alpha1.CollectorSchema{}, err
		}
	}

	sort.Sort(ByTypePriority(allImages))

	collectorSchema.AllImages = allImages
	collectorSchema.CopyImageSchemaMap = *copyImageSchemaMap

	return collectorSchema, nil
}

func isFullCatalog(catalog v2alpha1.Operator) bool {
	return len(catalog.IncludeConfig.Packages) == 0 && catalog.Full
}

func createFolders(paths []string) error {
	var errs []error
	for _, path := range paths {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(path, 0755)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func digestOfFilter(catalog v2alpha1.Operator) (string, error) {
	c := catalog
	c.TargetCatalog = ""
	c.TargetTag = ""
	c.TargetCatalogSourceTemplate = ""
	pkgs, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum(pkgs))[0:32], nil
}

func (o FilterCollector) isAlreadyFiltered(ctx context.Context, srcImage, filteredImageDigest string) bool {

	imgSpec, err := image.ParseRef(srcImage)
	if err != nil {
		o.Log.Debug(errMsg, err.Error())
		return false
	}

	sourceCtx, err := o.Opts.SrcImage.NewSystemContext()
	if err != nil {
		return false
	}
	// OCPBUGS-37948 : No TLS verification when getting manifests from the cache registry
	if strings.Contains(srcImage, o.Opts.LocalStorageFQDN) { // when copying from cache, use HTTP
		sourceCtx.DockerInsecureSkipTLSVerify = types.OptionalBoolTrue
	}

	catalogDigest, err := o.Manifest.GetDigest(ctx, sourceCtx, imgSpec.ReferenceWithTransport)
	if err != nil {
		o.Log.Debug(errMsg, err.Error())
		return false
	}
	return filteredImageDigest == catalogDigest
}
