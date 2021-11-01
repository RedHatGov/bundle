package publish

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/RedHatGov/bundle/pkg/operator"
	"github.com/containerd/containerd/errdefs"
	"github.com/openshift/library-go/pkg/image/reference"
	"github.com/openshift/oc/pkg/cli/image/imagesource"
	"github.com/operator-framework/operator-registry/alpha/action"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/pkg/image/containerdregistry"
	"github.com/sirupsen/logrus"
)

func (o *Options) rebuildCatalogs(ctx context.Context, dstDir string, filesInArchive map[string]string) (refs []imagesource.TypedImageReference, err error) {
	if err := unpack("catalogs", dstDir, filesInArchive); err != nil {
		nferr := &ErrArchiveFileNotFound{}
		if errors.As(err, &nferr) || errors.Is(err, os.ErrNotExist) {
			logrus.Debug("No catalogs found in archive, skipping catalog rebuild")
			return nil, nil
		}
		return nil, err
	}

	mirrorRef := imagesource.TypedImageReference{Type: imagesource.DestinationRegistry}
	if mirrorRef.Ref, err = reference.Parse(o.ToMirror); err != nil {
		return nil, err
	}

	dstDir = filepath.Clean(dstDir)
	catalogsByImage := map[imagesource.TypedImageReference]string{}
	if err := filepath.Walk(dstDir, func(fpath string, info fs.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return err
		}

		slashPath := filepath.ToSlash(fpath)
		if base := path.Base(slashPath); base == "index.json" {
			slashPath = strings.TrimPrefix(slashPath, fmt.Sprintf("%s/catalogs/", dstDir))
			regRepoNs, id := path.Split(path.Dir(slashPath))
			regRepoNs = path.Clean(regRepoNs)
			var img string
			if strings.Contains(id, ":") {
				// Digest.
				img = fmt.Sprintf("%s@%s", regRepoNs, id)
			} else {
				// Tag.
				img = fmt.Sprintf("%s:%s", regRepoNs, id)
			}
			ctlgRef := imagesource.TypedImageReference{Type: imagesource.DestinationRegistry}
			if ctlgRef.Ref, err = reference.Parse(img); err != nil {
				return fmt.Errorf("error parsing index dir path %q as image %q: %v", fpath, img, err)
			}
			// Update registry so the existing catalog image can be pulled.
			// QUESTION(estroz): is assuming an image is present in a repo with the same name valid?
			ctlgRef.Ref.Registry = mirrorRef.Ref.Registry
			catalogsByImage[ctlgRef] = filepath.Dir(fpath)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	resolver, err := containerdregistry.NewResolver("", o.SkipTLS, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating image resolver: %v", err)
	}
	reg, err := containerdregistry.NewRegistry(
		containerdregistry.SkipTLS(o.SkipTLS),
		containerdregistry.WithCacheDir(filepath.Join(dstDir, "cache")),
	)
	if err != nil {
		return nil, err
	}
	defer reg.Destroy()
	for ctlgRef, dcDir := range catalogsByImage {

		// An image for a particular catalog may not exist in the mirror registry yet,
		// ex. when publish is run for the first time for a catalog (full/headsonly).
		// If that is the case, then simply build the catalog image with the new
		// declarative config catalog; otherwise render the existing and new catalogs together.
		var dcDirToBuild string
		refExact := ctlgRef.Ref.Exact()
		if _, _, rerr := resolver.Resolve(ctx, refExact); rerr == nil {

			logrus.Infof("Catalog image %q found, rendering with new file-based catalog", refExact)

			dc, err := action.Render{
				// Order the old ctlgRef before dcDir so new packages/channels/bundles overwrite
				// existing counterparts.
				Refs:           []string{refExact, dcDir},
				AllowedRefMask: action.RefAll,
				Registry:       reg,
			}.Run(ctx)
			if err != nil {
				return nil, err
			}
			dcDirToBuild = filepath.Join(dcDir, "rendered")
			if err := os.MkdirAll(dcDirToBuild, os.ModePerm); err != nil {
				return nil, err
			}
			renderedPath := filepath.Join(dcDirToBuild, "index.json")
			f, err := os.Create(renderedPath)
			if err != nil {
				return nil, err
			}
			defer f.Close()
			if err := declcfg.WriteJSON(*dc, f); err != nil {
				return nil, err
			}

		} else if errors.Is(rerr, errdefs.ErrNotFound) {

			logrus.Infof("Catalog image %q not found, using new file-based catalog", refExact)
			dcDirToBuild = dcDir

		} else {
			return nil, fmt.Errorf("error resolving existing catalog image %q: %v", refExact, rerr)
		}

		// Build and push a new image with the same namespace, name, and optionally tag
		// as the original image, but to the mirror.
		if err := o.buildCatalogImage(ctx, ctlgRef.Ref, dstDir, dcDirToBuild); err != nil {
			return nil, fmt.Errorf("error building catalog image %q: %v", ctlgRef.Ref.Exact(), err)
		}

		// Resolve the image's digest for ICSP creation.
		_, desc, err := resolver.Resolve(ctx, ctlgRef.Ref.Exact())
		if err != nil {
			return nil, fmt.Errorf("error retrieving digest for catalog image %q: %v", ctlgRef.Ref.Exact(), err)
		}
		ctlgRef.Ref.ID = desc.Digest.String()

		refs = append(refs, ctlgRef)
	}

	return refs, nil
}

func (o *Options) buildCatalogImage(ctx context.Context, ref reference.DockerImageReference, dockerfileDir, dcDir string) error {

	dockerfile := filepath.Join(dockerfileDir, "index.Dockerfile")

	f, err := os.Create(dockerfile)
	if err != nil {
		return err
	}
	if err := (action.GenerateDockerfile{
		BaseImage: operator.OPMImage,
		IndexDir:  ".",
		Writer:    f,
	}).Run(); err != nil {
		return err
	}

	logrus.Infof("Building rendered catalog image: %s", ref.Exact())

	if len(o.BuildxPlatforms) == 0 {
		err = o.buildPodman(ctx, ref, dcDir, dockerfile)
	} else {
		err = o.buildDockerBuildx(ctx, ref, dcDir, dockerfile)
	}
	return err
}

func (o *Options) buildDockerBuildx(ctx context.Context, ref reference.DockerImageReference, dir, dockerfile string) error {
	exactRef := ref.Exact()

	args := []string{
		"build", "buildx",
		"-t", exactRef,
		"-f", dockerfile,
		"--platform", strings.Join(o.BuildxPlatforms, ","),
		"--push",
		dir,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := runDebug(cmd); err != nil {
		return err
	}

	return nil
}

func (o *Options) buildPodman(ctx context.Context, ref reference.DockerImageReference, dir, dockerfile string) error {
	exactRef := ref.Exact()

	bargs := []string{
		"build",
		"-t", exactRef,
		"-f", dockerfile,
		dir,
	}
	bcmd := exec.CommandContext(ctx, "podman", bargs...)
	bcmd.Stdout = os.Stdout
	bcmd.Stderr = os.Stderr
	if err := runDebug(bcmd); err != nil {
		return err
	}

	pargs := []string{
		"push",
		exactRef,
	}
	if o.SkipTLS {
		pargs = append(pargs, "--tls-verify=false")
	}
	pcmd := exec.CommandContext(ctx, "podman", pargs...)
	pcmd.Stdout = os.Stdout
	pcmd.Stderr = os.Stderr
	if err := runDebug(pcmd); err != nil {
		return err
	}

	return nil
}

func runDebug(cmd *exec.Cmd) error {
	logrus.Debugf("command: %s", strings.Join(cmd.Args, " "))
	return cmd.Run()
}
