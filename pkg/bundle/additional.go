package bundle

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/openshift/oc/pkg/cli/image/imagesource"
	"github.com/openshift/oc/pkg/cli/image/mirror"
	"github.com/sirupsen/logrus"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/RedHatGov/bundle/pkg/config"
	"github.com/RedHatGov/bundle/pkg/config/v1alpha1"
	"github.com/RedHatGov/bundle/pkg/image"
)

type AdditionalOptions struct {
	DestDir string
	DryRun  bool
	SkipTLS bool
}

func NewAdditionalOptions() *AdditionalOptions {
	return &AdditionalOptions{}
}

// GetAdditional downloads specified images in the imageset-config.yaml under mirror.additonalImages
func (o *AdditionalOptions) GetAdditional(_ v1alpha1.PastMirror, cfg v1alpha1.ImageSetConfiguration) (image.Associations, error) {

	stream := genericclioptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}

	opts := mirror.NewMirrorImageOptions(stream)
	opts.DryRun = o.DryRun
	opts.SecurityOptions.Insecure = o.SkipTLS
	opts.FileDir = filepath.Join(o.DestDir, config.SourceDir)

	logrus.Infof("Downloading %d image(s) to %s", len(cfg.Mirror.AdditionalImages), opts.FileDir)

	var mappings []mirror.Mapping
	images := make([]string, len(cfg.Mirror.AdditionalImages))
	assocMappings := make(map[string]string, len(cfg.Mirror.AdditionalImages))
	for i, img := range cfg.Mirror.AdditionalImages {

		// FIXME(jpower): need to have the user set skipVerification value
		// If the pullSecret is not empty create a cached context
		// else let `oc mirror` use the default docker config location
		if len(img.PullSecret) != 0 {
			ctx, err := config.CreateContext([]byte(img.PullSecret), false, o.SkipTLS)
			if err != nil {
				return nil, err
			}
			opts.SecurityOptions.CachedContext = ctx
		}

		// Get source image information
		srcRef, err := imagesource.ParseReference(img.Name)

		if err != nil {
			return nil, fmt.Errorf("error parsing source image %s: %v", img.Name, err)
		}

		// Set destination image information
		pathRef := "file://" + img.Name

		dstRef, err := imagesource.ParseReference(pathRef)
		if err != nil {
			return nil, fmt.Errorf("error parsing destination reference %s: %v", pathRef, err)
		}
		dstRef.Ref = dstRef.Ref.DockerClientDefaults()

		// Check if image is specified as a blocked image
		if IsBlocked(cfg, srcRef.Ref) {
			return nil, fmt.Errorf("additional image %s also specified as blocked, remove the image one config field or the other", img.Name)
		}
		// Create mapping from source and destination images
		mappings = append(mappings, mirror.Mapping{
			Source:      srcRef,
			Destination: dstRef,
			Name:        srcRef.Ref.Name,
		})

		// Add mapping and image for image association.
		// The registry component is not included in the final path.
		assocMappings[srcRef.String()] = "file://" + path.Join(dstRef.Ref.Namespace, dstRef.Ref.NameString())
		images[i] = srcRef.String()
	}

	opts.Mappings = mappings

	if err := opts.Run(); err != nil {
		return nil, err
	}

	return image.AssociateImageLayers(opts.FileDir, assocMappings, images)
}
