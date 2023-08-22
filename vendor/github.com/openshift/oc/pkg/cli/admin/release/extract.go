package release

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kcmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/openshift/library-go/pkg/image/dockerv1client"
	"github.com/openshift/library-go/pkg/manifest"
	"github.com/openshift/oc/pkg/cli/image/extract"
	"github.com/openshift/oc/pkg/cli/image/imagesource"
	imagemanifest "github.com/openshift/oc/pkg/cli/image/manifest"
	"github.com/openshift/oc/pkg/cli/image/workqueue"
	"github.com/pkg/errors"
)

var (
	credentialsRequestGVK = schema.GroupVersionKind{Group: "cloudcredential.openshift.io", Version: "v1", Kind: "CredentialsRequest"}

	credRequestCloudProviderSpecKindMapping = map[string]string{
		"alibabacloud": "AlibabaCloudProviderSpec",
		"aws":          "AWSProviderSpec",
		"azure":        "AzureProviderSpec",
		"gcp":          "GCPProviderSpec",
		"ibmcloud":     "IBMCloudProviderSpec",
		"nutanix":      "NutanixProviderSpec",
		"openstack":    "OpenStackProviderSpec",
		"ovirt":        "OvirtProviderSpec",
		"powervs":      "IBMCloudPowerVSProviderSpec",
		"vsphere":      "VSphereProviderSpec",
	}
)

// NewExtractOptions is also used internally as part of image mirroring. For image mirroring
// internal use, extractManifests is set to true so image manifest files are searched for
// signature information to be returned for use by mirroring.
func NewExtractOptions(streams genericclioptions.IOStreams, extractManifests bool) *ExtractOptions {
	return &ExtractOptions{
		IOStreams:        streams,
		Directory:        ".",
		ExtractManifests: extractManifests,
	}
}

func NewExtract(f kcmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	o := NewExtractOptions(streams, false)
	cmd := &cobra.Command{
		Use:   "extract",
		Short: "Extract the contents of an update payload to disk",
		Long: templates.LongDesc(`
			Extract the contents of a release image to disk.

			Extracts the contents of an OpenShift release image to disk for inspection or
			debugging. Update images contain manifests and metadata about the operators that
			must be installed on the cluster for a given version.

			The --tools and --command flags allow you to extract the appropriate client binaries
			for	your operating system to disk. --tools will create archive files containing the
			current OS tools (or, if --command-os is set to '*', all OS versions). Specifying
			--command for either 'oc' or 'openshift-install' will extract the binaries directly.
			You may pass a PGP private key file with --signing-key which will create an ASCII
			armored sha256sum.txt.asc file describing the content that was extracted that is
			signed by the key. For more advanced signing, use the generated sha256sum.txt and an
			external tool like gpg.

			The --credentials-requests flag filters extracted manifests to only cloud credential
			requests. The --cloud flag further filters credential requests to a specific cloud.
			Valid values for --cloud include alibabacloud, aws, azure, gcp, ibmcloud, nutanix, openstack, ovirt, powervs, and vsphere.

			Instead of extracting the manifests, you can specify --git=DIR to perform a Git
			checkout of the source code that comprises the release. A warning will be printed
			if the component is not associated with source code. The command will not perform
			any destructive actions on your behalf except for executing a 'git checkout' which
			may change the current branch. Requires 'git' to be on your path.

			If the specified image supports multiple operating systems, the image that matches the
			current operating system will be chosen. Otherwise you must pass --filter-by-os to
			select the desired image.
		`),
		Example: templates.Examples(`
			# Use git to check out the source code for the current cluster release to DIR
			oc adm release extract --git=DIR

			# Extract cloud credential requests for AWS
			oc adm release extract --credentials-requests --cloud=aws

			# Use git to check out the source code for the current cluster release to DIR from linux/s390x image
			# Note: Wildcard filter is not supported. Pass a single os/arch to extract
			oc adm release extract --git=DIR quay.io/openshift-release-dev/ocp-release:4.11.2 --filter-by-os=linux/s390x
		`),
		Run: func(cmd *cobra.Command, args []string) {
			kcmdutil.CheckErr(o.Complete(f, cmd, args))
			kcmdutil.CheckErr(o.Validate())
			kcmdutil.CheckErr(o.Run())
		},
	}
	flags := cmd.Flags()
	o.SecurityOptions.Bind(flags)
	o.FilterOptions.Bind(flags)
	o.ParallelOptions.Bind(flags)

	flags.StringVar(&o.ICSPFile, "icsp-file", o.ICSPFile, "Path to an ImageContentSourcePolicy file. If set, data from this file will be used to find alternative locations for images.")

	flags.StringVar(&o.From, "from", o.From, "Image containing the release payload.")
	flags.StringVar(&o.File, "file", o.File, "Extract a single file from the payload to standard output.")
	flags.StringVar(&o.Directory, "to", o.Directory, "Directory to write release contents to, defaults to the current directory.")

	flags.StringVar(&o.GitExtractDir, "git", o.GitExtractDir, "Check out the sources that created this release into the provided dir. Repos will be created at <dir>/<host>/<path>. Requires 'git' on your path.")
	flags.BoolVar(&o.Tools, "tools", o.Tools, "Extract the tools archives from the release image. Implies --command=*")
	flags.StringVar(&o.SigningKey, "signing-key", o.SigningKey, "Sign the sha256sum.txt generated by --tools with this GPG key. A sha256sum.txt.asc file signed by this key will be created. The key is assumed to be encrypted.")

	flags.StringVar(&o.Command, "command", o.Command, "Specify 'oc' or 'openshift-install' to extract the client for your operating system.")
	flags.StringVar(&o.CommandOperatingSystem, "command-os", o.CommandOperatingSystem, "Override which operating system command is extracted (mac, windows, linux) or can be specified with arch(linux/arm64, mac/amd64). You map specify '*' to extract all tool archives.")
	flags.StringVar(&o.FileDir, "dir", o.FileDir, "The directory on disk that file:// images will be copied under.")

	flags.BoolVar(&o.CredentialsRequests, "credentials-requests", o.CredentialsRequests, "Extract credential request manifests only")
	flags.StringVar(&o.Cloud, "cloud", o.Cloud, "Specify the cloud for which credential request manifests should be extracted. Works only in combination with --credentials-requests.")

	flags.StringVarP(&o.Output, "output", "o", o.Output, "Output format. Supports 'commit' when used with '--git'.")
	return cmd
}

type ExtractOptions struct {
	genericclioptions.IOStreams

	SecurityOptions imagemanifest.SecurityOptions
	FilterOptions   imagemanifest.FilterOptions
	ParallelOptions imagemanifest.ParallelOptions

	ICSPFile string

	Output string

	FromDir string
	From    string

	Tools                  bool
	Command                string
	CommandOperatingSystem string
	SigningKey             string

	// CredentialsRequests if true, results in only credential request manifests getting extracted.
	// If Cloud is specified, then only the credential requests for that cloud are extracted.
	CredentialsRequests bool
	Cloud               string

	// GitExtractDir is the path of a root directory to extract the source of a release to.
	GitExtractDir string

	Directory string
	File      string
	FileDir   string

	ExtractManifests bool
	Manifests        []manifest.Manifest

	ImageMetadataCallback extract.ImageMetadataFunc
}

func (o *ExtractOptions) Complete(f kcmdutil.Factory, cmd *cobra.Command, args []string) error {
	switch {
	case len(args) == 1 && len(o.From) > 0, len(args) > 1:
		return fmt.Errorf("you may only specify a single image via --from or argument")
	}
	if len(o.From) > 0 {
		args = []string{o.From}
	}
	args, err := findArgumentsFromCluster(f, args)
	if err != nil {
		return err
	}
	if len(args) != 1 {
		return fmt.Errorf("you may only specify a single image via --from or argument")
	}
	o.From = args[0]

	return o.FilterOptions.Complete(cmd.Flags())
}

func (o *ExtractOptions) Validate() error {
	return o.FilterOptions.Validate()
}

func (o *ExtractOptions) Run() error {
	sources := 0
	if o.Tools {
		sources++
	}
	if o.CredentialsRequests {
		sources++
	}
	if len(o.File) > 0 {
		sources++
	}
	if len(o.Command) > 0 {
		sources++
	}
	if len(o.GitExtractDir) > 0 {
		sources++
	}

	if len(o.Output) > 0 && len(o.GitExtractDir) == 0 {
		return fmt.Errorf("--output is only supported with --git")
	}

	if !o.CredentialsRequests && len(o.Cloud) > 0 {
		return fmt.Errorf("--cloud is only supported with --credentials-requests")
	}
	if len(o.Cloud) > 0 {
		if _, ok := credRequestCloudProviderSpecKindMapping[o.Cloud]; !ok {
			return fmt.Errorf("--cloud value not recognized, must be one of: %v", validCloudValues())
		}
	}

	switch {
	case sources > 1:
		return fmt.Errorf("only one of --tools, --command, --credentials-requests, --file, or --git may be specified")
	case len(o.From) == 0:
		return fmt.Errorf("must specify an image containing a release payload with --from")
	case o.Directory != "." && len(o.File) > 0:
		return fmt.Errorf("only one of --to and --file may be set")

	case len(o.GitExtractDir) > 0:
		return o.extractGit(o.GitExtractDir)
	case o.Tools:
		return o.extractTools()
	case len(o.Command) > 0:
		return o.extractCommand(o.Command)
	}

	dir := o.Directory
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	src := o.From
	ref, err := imagesource.ParseReference(src)
	if err != nil {
		return err
	}
	opts := extract.NewExtractOptions(genericclioptions.IOStreams{Out: o.Out, ErrOut: o.ErrOut})
	opts.ParallelOptions = o.ParallelOptions
	opts.SecurityOptions = o.SecurityOptions
	opts.FilterOptions = o.FilterOptions
	opts.FileDir = o.FileDir
	opts.OnlyFiles = true
	opts.ICSPFile = o.ICSPFile
	opts.Mappings = []extract.Mapping{
		{
			ImageRef: ref,

			From: "release-manifests/",
			To:   dir,
		},
	}

	imageMetadataCallbacks := []extract.ImageMetadataFunc{}
	if o.ImageMetadataCallback != nil {
		imageMetadataCallbacks = append(imageMetadataCallbacks, o.ImageMetadataCallback)
	}

	var metadataVerifyMsg string

	verifier := imagemanifest.NewVerifier()
	imageMetadataCallbacks = append(imageMetadataCallbacks, func(m *extract.Mapping, dgst, contentDigest digest.Digest, config *dockerv1client.DockerImageConfig, manifestListDigest digest.Digest) {
		verifier.Verify(dgst, contentDigest)
		if len(ref.Ref.ID) > 0 {
			metadataVerifyMsg = fmt.Sprintf("Extracted release payload created at %s", config.Created.Format(time.RFC3339))
		} else {
			metadataVerifyMsg = fmt.Sprintf("Extracted release payload from digest %s created at %s", dgst, config.Created.Format(time.RFC3339))
		}
	})

	opts.ImageMetadataCallback = func(m *extract.Mapping, dgst, contentDigest digest.Digest, config *dockerv1client.DockerImageConfig, manifestListDigest digest.Digest) {
		for _, callback := range imageMetadataCallbacks {
			callback(m, dgst, contentDigest, config, manifestListDigest)
		}
	}

	switch {
	case len(o.File) > 0:
		var manifestErrs []error
		found := false
		opts.TarEntryCallback = func(hdr *tar.Header, _ extract.LayerInfo, r io.Reader) (bool, error) {
			if !o.ExtractManifests {
				if hdr.Name != o.File {
					return true, nil
				}
				if _, err := io.Copy(o.Out, r); err != nil {
					return false, err
				}
				found = true
				return false, nil
			} else {
				switch hdr.Name {
				case o.File:
					if _, err := io.Copy(o.Out, r); err != nil {
						return false, err
					}
					found = true
				case "image-references":
					return true, nil
				case "release-metadata":
					return true, nil
				default:
					if ext := path.Ext(hdr.Name); len(ext) == 0 || !(ext == ".yaml" || ext == ".yml" || ext == ".json") {
						return true, nil
					}
					klog.V(4).Infof("Found manifest %s", hdr.Name)
					raw, err := ioutil.ReadAll(r)
					if err != nil {
						manifestErrs = append(manifestErrs, errors.Wrapf(err, "error reading file %s", hdr.Name))
						return true, nil
					}
					ms, err := manifest.ParseManifests(bytes.NewReader(raw))
					if err != nil {
						manifestErrs = append(manifestErrs, errors.Wrapf(err, "error parsing %s", hdr.Name))
						return true, nil
					}
					o.Manifests = append(o.Manifests, ms...)
				}
				return true, nil
			}
		}
		if err := opts.Run(); err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("image did not contain %s", o.File)
		}

		// Only output manifest errors if manifests were being extracted.
		// Do not return an error so current operation, e.g. mirroring, continues.
		if o.ExtractManifests && len(manifestErrs) > 0 {
			fmt.Fprintf(o.ErrOut, "Errors: %s\n", errorList(manifestErrs))
		}
		return nil

	case o.CredentialsRequests:
		expectedProviderSpecKind := ""
		if len(o.Cloud) > 0 {
			expectedProviderSpecKind = credRequestCloudProviderSpecKindMapping[o.Cloud]
		}
		opts.TarEntryCallback = func(hdr *tar.Header, _ extract.LayerInfo, r io.Reader) (bool, error) {
			if ext := path.Ext(hdr.Name); len(ext) == 0 || !(ext == ".yaml" || ext == ".yml" || ext == ".json") {
				return true, nil
			}
			klog.V(4).Infof("Found manifest %s", hdr.Name)
			raw, err := ioutil.ReadAll(r)
			if err != nil {
				return false, errors.Wrapf(err, "error reading file %s", hdr.Name)
			}
			ms, err := manifest.ParseManifests(bytes.NewReader(raw))
			if err != nil {
				return false, errors.Wrapf(err, "error parsing %s", hdr.Name)
			}
			credRequestManifests := []manifest.Manifest{}
			for _, m := range ms {
				if m.GVK != credentialsRequestGVK {
					continue
				}
				if len(expectedProviderSpecKind) > 0 {
					kind, _, err := unstructured.NestedString(m.Obj.Object, "spec", "providerSpec", "kind")
					if err != nil {
						return false, errors.Wrap(err, "error extracting cred request kind")
					}
					if kind != expectedProviderSpecKind {
						continue
					}
				}
				credRequestManifests = append(credRequestManifests, m)
			}
			if len(credRequestManifests) == 0 {
				return true, nil
			}

			out := o.Out
			if len(o.Directory) > 0 {
				out, err = os.Create(filepath.Join(o.Directory, hdr.Name))
				if err != nil {
					return false, errors.Wrapf(err, "error creating manifest in %s", hdr.Name)
				}
			}
			for _, m := range credRequestManifests {
				yamlBytes, err := yaml.JSONToYAML(m.Raw)
				if err != nil {
					return false, errors.Wrapf(err, "error serializing manifest in %s", hdr.Name)
				}
				fmt.Fprintf(out, "---\n")
				if _, err := out.Write(yamlBytes); err != nil {
					return false, errors.Wrapf(err, "error writing manifest in %s", hdr.Name)
				}
			}
			return true, nil
		}
		if err := opts.Run(); err != nil {
			return err
		}
	default:
		if err := opts.Run(); err != nil {
			return err
		}
	}

	if metadataVerifyMsg != "" {
		fmt.Fprintf(o.Out, "%s\n", metadataVerifyMsg)
	}

	if !verifier.Verified() {
		err := fmt.Errorf("the release image failed content verification and may have been tampered with")
		if !o.SecurityOptions.SkipVerification {
			return err
		}
		fmt.Fprintf(o.ErrOut, "warning: %v\n", err)
	}
	return nil

}

func (o *ExtractOptions) extractGit(dir string) error {
	switch o.Output {
	case "commit", "":
	default:
		return fmt.Errorf("the only supported option for --output is 'commit'")
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	opts := NewInfoOptions(o.IOStreams)
	opts.SecurityOptions = o.SecurityOptions
	opts.FilterOptions = o.FilterOptions
	opts.FileDir = o.FileDir
	release, err := opts.LoadReleaseInfo(o.From, false)
	if err != nil {
		return err
	}

	hadErrors := false
	var once sync.Once
	alreadyExtracted := make(map[string]string)
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	q := workqueue.New(8, ctx.Done())
	q.Batch(func(w workqueue.Work) {
		for _, ref := range release.References.Spec.Tags {
			repo := ref.Annotations[annotationBuildSourceLocation]
			commit := ref.Annotations[annotationBuildSourceCommit]
			if len(repo) == 0 || len(commit) == 0 {
				if klog.V(2).Enabled() {
					klog.Infof("Tag %s has no source info", ref.Name)
				} else {
					fmt.Fprintf(o.ErrOut, "warning: Tag %s has no source info\n", ref.Name)
				}
				continue
			}
			if oldCommit, ok := alreadyExtracted[repo]; ok {
				if oldCommit != commit {
					fmt.Fprintf(o.ErrOut, "warning: Repo %s referenced more than once with different commits, only checking out the first reference\n", repo)
				}
				continue
			}
			alreadyExtracted[repo] = commit

			w.Parallel(func() {
				buf := &bytes.Buffer{}
				extractedRepo, err := ensureCloneForRepo(dir, repo, nil, buf, buf)
				if err != nil {
					once.Do(func() { hadErrors = true })
					fmt.Fprintf(o.ErrOut, "error: cloning %s: %v\n%s\n", repo, err, buf.String())
					return
				}

				switch o.Output {
				case "commit":
					klog.V(2).Infof("Checkout %s from %s ...", commit, repo)
					buf.Reset()
					ok, err := extractedRepo.VerifyCommit(repo, commit)
					if err != nil {
						once.Do(func() { hadErrors = true })
						fmt.Fprintf(o.ErrOut, "error: could not find commit %s in %s: %v\n%s\n", commit, repo, err, buf.String())
						return
					}
					if !ok {
						once.Do(func() { hadErrors = true })
						fmt.Fprintf(o.ErrOut, "error: could not find commit %s in %s", commit, repo)
						return
					}
					fmt.Fprintf(o.Out, "%s %s\n", extractedRepo.path, commit)

				case "":
					klog.V(2).Infof("Checkout %s from %s ...", commit, repo)
					buf.Reset()
					if err := extractedRepo.CheckoutCommit(repo, commit); err != nil {
						once.Do(func() { hadErrors = true })
						fmt.Fprintf(o.ErrOut, "error: checking out commit for %s: %v\n%s\n", repo, err, buf.String())
						return
					}
					fmt.Fprintf(o.Out, "%s\n", extractedRepo.path)
				}
			})
		}
	})
	if hadErrors {
		return kcmdutil.ErrExit
	}
	return nil
}

func validCloudValues() []string {
	values := make([]string, 0, len(credRequestCloudProviderSpecKindMapping))
	for k := range credRequestCloudProviderSpecKindMapping {
		values = append(values, k)
	}
	return values
}
