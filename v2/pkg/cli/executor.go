package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"k8s.io/kubectl/pkg/util/templates"

	"github.com/distribution/distribution/v3/configuration"
	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	distversion "github.com/distribution/distribution/v3/version"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/openshift/oc-mirror/v2/pkg/additional"
	"github.com/openshift/oc-mirror/v2/pkg/api/v2alpha1"
	"github.com/openshift/oc-mirror/v2/pkg/archive"
	"github.com/openshift/oc-mirror/v2/pkg/batch"
	"github.com/openshift/oc-mirror/v2/pkg/clusterresources"
	"github.com/openshift/oc-mirror/v2/pkg/config"
	"github.com/openshift/oc-mirror/v2/pkg/delete"
	"github.com/openshift/oc-mirror/v2/pkg/image"
	"github.com/openshift/oc-mirror/v2/pkg/imagebuilder"
	clog "github.com/openshift/oc-mirror/v2/pkg/log"
	"github.com/openshift/oc-mirror/v2/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/pkg/mirror"
	"github.com/openshift/oc-mirror/v2/pkg/operator"
	"github.com/openshift/oc-mirror/v2/pkg/release"
	"github.com/openshift/oc-mirror/v2/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	mirrorlongDesc = templates.LongDesc(
		` 
		Create and publish user-configured mirrors with a declarative configuration input.
		used for authenticating to the registries. 

		The podman location for credentials is also supported as a secondary location.

		1. Destination prefix is docker:// - The current working directory will be used.
		2. Destination prefix is oci:// - The destination directory specified will be used.

		`,
	)
	mirrorExamples = templates.Examples(
		`
		# Mirror to a directory
		oc-mirror oci:mirror --config mirror-config.yaml
		`,
	)
)

type ExecutorSchema struct {
	Log                          clog.PluggableLoggerInterface
	LogsDir                      string
	registryLogFile              *os.File
	Config                       v2alpha1.ImageSetConfiguration
	Opts                         *mirror.CopyOptions
	WorkingDir                   string
	Operator                     operator.CollectorInterface
	Release                      release.CollectorInterface
	AdditionalImages             additional.CollectorInterface
	Mirror                       mirror.MirrorInterface
	Manifest                     manifest.ManifestInterface
	Batch                        batch.BatchInterface
	LocalStorageService          registry.Registry
	localStorageInterruptChannel chan error
	LocalStorageFQDN             string
	LocalStorageDisk             string
	ClusterResources             clusterresources.GeneratorInterface
	ImageBuilder                 imagebuilder.ImageBuilderInterface
	MirrorArchiver               archive.Archiver
	MirrorUnArchiver             archive.UnArchiver
	MakeDir                      MakeDirInterface
	Delete                       delete.DeleteInterface
	srcFlagSet                   pflag.FlagSet // this is used so that we can set tlsVerify for the cache registry based on Mode (which is initialized in Complete func)
	destFlagSet                  pflag.FlagSet // this is used so that we can set tlsVerify for the cache registry based on Mode (which is initialized in Complete func)
}

type MakeDirInterface interface {
	makeDirAll(string, os.FileMode) error
}

type MakeDir struct {
}

func (o MakeDir) makeDirAll(dir string, mode os.FileMode) error {
	return os.MkdirAll(dir, mode)
}

// NewMirrorCmd - cobra entry point
func NewMirrorCmd(log clog.PluggableLoggerInterface) *cobra.Command {

	global := &mirror.GlobalOptions{
		SecurePolicy: false,
	}

	flagSharedOpts, sharedOpts := mirror.SharedImageFlags()
	flagDepTLS, deprecatedTLSVerifyOpt := mirror.DeprecatedTLSVerifyFlags()
	flagSrcOpts, srcOpts := mirror.ImageSrcFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "src-", "screds")
	flagDestOpts, destOpts := mirror.ImageDestFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "dest-", "dcreds")
	flagRetryOpts, retryOpts := mirror.RetryFlags()

	opts := &mirror.CopyOptions{
		Global:               global,
		DeprecatedTLSVerify:  deprecatedTLSVerifyOpt,
		SrcImage:             srcOpts,
		DestImage:            destOpts,
		RetryOpts:            retryOpts,
		Dev:                  false,
		MaxParallelDownloads: 1000,
		Function:             string(mirror.CopyMode),
	}

	mkd := MakeDir{}
	ex := &ExecutorSchema{
		srcFlagSet:  flagSrcOpts,
		destFlagSet: flagDestOpts,
		Log:         log,
		Opts:        opts,
		MakeDir:     mkd,
	}

	cmd := &cobra.Command{
		Use:           fmt.Sprintf("%v <destination type>:<destination location>", filepath.Base(os.Args[0])),
		Version:       "v2.0.0-dev-01",
		Short:         "Manage mirrors per user configuration",
		Long:          mirrorlongDesc,
		Example:       mirrorExamples,
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: false,
		SilenceUsage:  false,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("👋 Hello, welcome to oc-mirror")
			log.Info("⚙️  setting up the environment for you...")

			err := ex.Validate(args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
			err = ex.Complete(args)
			if err != nil {
				log.Error(" %v ", err)
				os.Exit(1)
			}
			// prepare internal storage
			err = ex.setupLocalStorage()
			if err != nil {
				log.Error(" %v ", err)
				os.Exit(1)
			}

			err = ex.Run(cmd, args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
		},
	}
	cmd.AddCommand(version.NewVersionCommand(log))
	cmd.AddCommand(NewDeleteCommand(log))
	cmd.PersistentFlags().StringVarP(&opts.Global.ConfigPath, "config", "c", "", "Path to imageset configuration file")
	cmd.Flags().StringVar(&opts.Global.LogLevel, "loglevel", "info", "Log level one of (info, debug, trace, error)")
	cmd.Flags().StringVar(&opts.Global.WorkingDir, "workspace", "", "oc-mirror workspace where resources and internal artifacts are generated")
	cmd.Flags().StringVar(&opts.Global.From, "from", "", "local storage directory for disk to mirror workflow")
	cmd.Flags().Uint16VarP(&opts.Global.Port, "port", "p", 55000, "HTTP port used by oc-mirror's local storage instance")
	cmd.Flags().BoolVarP(&opts.Global.Quiet, "quiet", "q", false, "enable detailed logging when copying images")
	cmd.Flags().BoolVarP(&opts.Global.Force, "force", "f", false, "force the copy and mirror functionality")
	cmd.Flags().BoolVarP(&opts.IsDryRun, "dry-run", "", false, "Print actions without mirroring images")
	cmd.Flags().BoolVar(&opts.Global.V2, "v2", opts.Global.V2, "Redirect the flow to oc-mirror v2 - This is Tech Preview, it is still under development and it is not production ready.")
	cmd.Flags().BoolVar(&opts.Global.SecurePolicy, "secure-policy", opts.Global.SecurePolicy, "If set (default is false), will enable signature verification (secure policy for signature verification).")
	cmd.Flags().IntVar(&opts.Global.MaxNestedPaths, "max-nested-paths", 0, "Number of nested paths, for destination registries that limit nested paths")
	cmd.Flags().BoolVar(&opts.Global.StrictArchiving, "strict-archive", false, "// If set, generates archives that are strictly less than `archiveSize`, failing for files that exceed that limit.")
	cmd.Flags().StringVar(&opts.Global.SinceString, "since", "", "Include all new content since specified date (format yyyy-MM-dd). When not provided, new content since previous mirroring is mirrored")
	// nolint: errcheck
	cmd.Flags().AddFlagSet(&flagSharedOpts)
	cmd.Flags().AddFlagSet(&flagRetryOpts)
	cmd.Flags().AddFlagSet(&flagDepTLS)
	cmd.Flags().AddFlagSet(&flagSrcOpts)
	cmd.Flags().AddFlagSet(&flagDestOpts)
	HideFlags(cmd)

	ex.Opts.Stdout = cmd.OutOrStdout()

	return cmd
}

// nolint: errcheck
func HideFlags(cmd *cobra.Command) {
	cmd.Flags().MarkHidden("v2")
	cmd.Flags().MarkHidden("dest-authfile")
	cmd.Flags().MarkHidden("dest-cert-dir")
	cmd.Flags().MarkHidden("dest-compress")
	cmd.Flags().MarkHidden("dest-compress-format")
	cmd.Flags().MarkHidden("dest-compress-level")
	cmd.Flags().MarkHidden("dest-creds")
	cmd.Flags().MarkHidden("dest-daemon-host")
	cmd.Flags().MarkHidden("dest-decompress")
	cmd.Flags().MarkHidden("dest-no-creds")
	cmd.Flags().MarkHidden("dest-oci-accept-uncompressed-layers")
	cmd.Flags().MarkHidden("dest-password")
	cmd.Flags().MarkHidden("dest-precompute-digests")
	cmd.Flags().MarkHidden("dest-registry-token")
	cmd.Flags().MarkHidden("dest-shared-blob-dir")
	cmd.Flags().MarkHidden("dest-username")
	cmd.Flags().MarkHidden("dir")
	cmd.Flags().MarkHidden("force")
	cmd.Flags().MarkHidden("quiet")
	cmd.Flags().MarkHidden("retry-times")
	cmd.Flags().MarkHidden("src-authfile")
	cmd.Flags().MarkHidden("src-cert-dir")
	cmd.Flags().MarkHidden("src-creds")
	cmd.Flags().MarkHidden("src-daemon-host")
	cmd.Flags().MarkHidden("src-no-creds")
	cmd.Flags().MarkHidden("src-password")
	cmd.Flags().MarkHidden("src-registry-token")
	cmd.Flags().MarkHidden("src-shared-blob-dir")
	cmd.Flags().MarkHidden("src-username")
}

// Validate - cobra validation
func (o ExecutorSchema) Validate(dest []string) error {
	if len(o.Opts.Global.ConfigPath) == 0 {
		return fmt.Errorf("use the --config flag it is mandatory")
	}
	if strings.Contains(dest[0], fileProtocol) && o.Opts.Global.From != "" {
		return fmt.Errorf("when destination is file://, mirrorToDisk workflow is assumed, and the --from argument is not needed")
	}
	if len(o.Opts.Global.From) > 0 && !strings.Contains(o.Opts.Global.From, fileProtocol) {
		return fmt.Errorf("when --from is used, it must have file:// prefix")
	}
	if len(o.Opts.Global.From) > 0 && o.Opts.Global.SinceString != "" {
		o.Log.Warn("since flag is only taken into account during mirrorToDisk workflow")
	}
	if o.Opts.Global.SinceString != "" {
		if _, err := time.Parse(time.DateOnly, o.Opts.Global.SinceString); err != nil {
			return fmt.Errorf("--since flag needs to be in format yyyy-MM-dd")
		}
	}
	if strings.Contains(dest[0], fileProtocol) && o.Opts.Global.WorkingDir != "" {
		return fmt.Errorf("when destination is file://, mirrorToDisk workflow is assumed, and the --workspace argument is not needed")
	}
	if strings.Contains(dest[0], dockerProtocol) && o.Opts.Global.WorkingDir != "" && o.Opts.Global.From != "" {
		return fmt.Errorf("when destination is docker://, --from (assumes disk to mirror workflow) and --workspace (assumes mirror to mirror workflow) cannot be used together")
	}
	if strings.Contains(dest[0], dockerProtocol) && o.Opts.Global.WorkingDir == "" && o.Opts.Global.From == "" {
		return fmt.Errorf("when destination is docker://, either --from (assumes disk to mirror workflow) or --workspace (assumes mirror to mirror workflow) need to be provided")
	}
	if len(o.Opts.Global.WorkingDir) > 0 && !strings.Contains(o.Opts.Global.WorkingDir, fileProtocol) {
		return fmt.Errorf("when --workspace is used, it must have file:// prefix")
	}
	if strings.Contains(dest[0], fileProtocol) || strings.Contains(dest[0], dockerProtocol) {
		return nil
	} else {
		return fmt.Errorf("destination must have either file:// (mirror to disk) or docker:// (diskToMirror) protocol prefixes")
	}
}

// Complete - do the final setup of modules
func (o *ExecutorSchema) Complete(args []string) error {

	if envOverride, ok := os.LookupEnv("CONTAINERS_REGISTRIES_CONF"); ok {
		o.Opts.Global.RegistriesConfPath = envOverride
	}

	o.Log.Debug("imagesetconfig file %s ", o.Opts.Global.ConfigPath)
	// read the ImageSetConfiguration
	cfg, err := config.ReadConfig(o.Opts.Global.ConfigPath, v2alpha1.ImageSetConfigurationKind)
	if err != nil {
		return err
	}
	o.Log.Debug("imagesetconfig : %v ", cfg)

	// update all dependant modules
	mc := mirror.NewMirrorCopy()
	md := mirror.NewMirrorDelete()
	o.Manifest = manifest.New(o.Log)
	o.Mirror = mirror.New(mc, md)
	o.Config = cfg.(v2alpha1.ImageSetConfiguration)

	// logic to check mode
	var rootDir string
	if strings.Contains(args[0], fileProtocol) {
		o.Opts.Mode = mirror.MirrorToDisk
		rootDir = strings.TrimPrefix(args[0], fileProtocol)
		o.Log.Debug("destination %s ", rootDir)
		// destination is the local cache, which is HTTP
		o.destFlagSet.Set("dest-tls-verify", "false")
	} else if strings.Contains(args[0], dockerProtocol) && o.Opts.Global.From != "" {
		rootDir = strings.TrimPrefix(o.Opts.Global.From, fileProtocol)
		o.Opts.Mode = mirror.DiskToMirror
		// source is the local cache, which is HTTP
		o.srcFlagSet.Set("src-tls-verify", "false")
	} else if strings.Contains(args[0], dockerProtocol) && o.Opts.Global.From == "" {
		o.Opts.Mode = mirror.MirrorToMirror
		if o.Opts.Global.WorkingDir == "" { // this should have been caught by Validate function. Nevertheless...
			return fmt.Errorf("mirror to mirror workflow detected. --workspace is mandatory to provide in the command arguments")
		}
		o.Opts.Global.WorkingDir = strings.TrimPrefix(o.Opts.Global.WorkingDir, fileProtocol)
	} else {
		o.Log.Error("unable to determine the mode (the destination must be either file:// or docker://)")
	}
	o.Opts.Destination = args[0]
	if o.Opts.Global.WorkingDir == "" { // this can already be set by using flag --workspace in mirror to mirror workflow
		o.Opts.Global.WorkingDir = filepath.Join(rootDir, workingDir)
	} else {
		o.Opts.Global.WorkingDir = strings.TrimPrefix(o.Opts.Global.WorkingDir, fileProtocol)
		if filepath.Base(o.Opts.Global.WorkingDir) != workingDir {
			o.Opts.Global.WorkingDir = filepath.Join(o.Opts.Global.WorkingDir, workingDir)
		}
	}
	o.Log.Info("🔀 workflow mode: %s ", o.Opts.Mode)

	if o.Opts.Global.SinceString != "" {
		o.Opts.Global.Since, err = time.Parse(time.DateOnly, o.Opts.Global.SinceString)
		if err != nil {
			// this should not happen, as should be caught by Validate
			return fmt.Errorf("unable to parse since flag: %v. Expected format is yyyy-MM.dd", err)
		}
	}

	// make sure we always get multi-arch images
	o.Opts.MultiArch = "all"
	// for the moment, mirroring doesn't verify signatures. Expected in CLID-26
	o.Opts.RemoveSignatures = true

	// setup logs level, and logsDir under workingDir
	err = o.setupLogsLevelAndDir()
	if err != nil {
		return err
	}

	if o.isLocalStoragePortBound() {
		return fmt.Errorf("%d is already bound and cannot be used", o.Opts.Global.Port)
	}
	o.LocalStorageFQDN = "localhost:" + strconv.Itoa(int(o.Opts.Global.Port))

	err = o.setupWorkingDir()
	if err != nil {
		return err
	}

	err = o.setupLocalStorageDir()
	if err != nil {
		return err
	}

	client, _ := release.NewOCPClient(uuid.New())

	o.ImageBuilder = imagebuilder.NewBuilder(o.Log, *o.Opts)

	signature := release.NewSignatureClient(o.Log, o.Config, *o.Opts)
	cn := release.NewCincinnati(o.Log, &o.Config, *o.Opts, client, false, signature)
	o.Release = release.New(o.Log, o.LogsDir, o.Config, *o.Opts, o.Mirror, o.Manifest, cn, o.LocalStorageFQDN, o.ImageBuilder)
	o.Operator = operator.New(o.Log, o.LogsDir, o.Config, *o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)
	o.AdditionalImages = additional.New(o.Log, o.Config, *o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)
	o.ClusterResources = clusterresources.New(o.Log, o.Opts.Global.WorkingDir, o.Config)
	o.Batch = batch.New(o.Log, o.LogsDir, o.Mirror, o.Manifest)

	if o.Opts.IsMirrorToDisk() {
		if o.Opts.Global.StrictArchiving {
			o.MirrorArchiver, err = archive.NewMirrorArchive(o.Opts, rootDir, o.Opts.Global.ConfigPath, o.Opts.Global.WorkingDir, o.LocalStorageDisk, o.Config.ImageSetConfigurationSpec.ArchiveSize, o.Log)
			if err != nil {
				return err
			}
		} else {
			o.MirrorArchiver, err = archive.NewPermissiveMirrorArchive(o.Opts, rootDir, o.Opts.Global.ConfigPath, o.Opts.Global.WorkingDir, o.LocalStorageDisk, o.Config.ImageSetConfigurationSpec.ArchiveSize, o.Log)
			if err != nil {
				return err
			}
		}
	} else if o.Opts.IsDiskToMirror() { // if added so that the unArchiver is not instanciated for the prepare workflow
		o.MirrorUnArchiver, err = archive.NewArchiveExtractor(rootDir, o.Opts.Global.WorkingDir, o.LocalStorageDisk)
		if err != nil {
			return err
		}
	}
	return nil
}

// Run - start the mirror functionality
func (o *ExecutorSchema) Run(cmd *cobra.Command, args []string) error {
	var err error

	switch {
	case o.Opts.IsMirrorToDisk():
		err = o.RunMirrorToDisk(cmd, args)
	case o.Opts.IsDiskToMirror():
		err = o.RunDiskToMirror(cmd, args)
	default:
		err = o.RunMirrorToMirror(cmd, args)
	}

	o.Log.Info("👋 Goodbye, thank you for using oc-mirror")

	if err != nil {
		o.closeAll()
		return err
	}

	defer o.closeAll()
	return nil
}

// setupLocalRegistryConfig - private function to parse registry config
// used in both localregistry serve and localregistry garbage-collect (for delete)
func (o *ExecutorSchema) setupLocalRegistryConfig() (*configuration.Configuration, error) {
	// create config file for local registry
	// sonarqube scanner variable declaration convention
	configYamlV01 := `
version: 0.1
log:
  accesslog:
    disabled: {{ .LogAccessOff }}
  level: {{ .LogLevel }}
  formatter: text
  fields:
    service: registry
storage:
  delete:
    enabled: true
  cache:
    blobdescriptor: inmemory
  filesystem:
    rootdirectory: {{ .LocalStorageDisk }}
http:
  addr: :{{ .LocalStoragePort }}
  headers:
    X-Content-Type-Options: [nosniff]
      #auth:
      #htpasswd:
      #realm: basic-realm
      #path: /etc/registry
`

	var buff bytes.Buffer
	type RegistryConfig struct {
		LocalStorageDisk string
		LocalStoragePort int
		LogLevel         string
		LogAccessOff     bool
	}

	rc := RegistryConfig{
		LocalStorageDisk: o.LocalStorageDisk,
		LocalStoragePort: int(o.Opts.Global.Port),
		LogLevel:         o.Opts.Global.LogLevel,
		LogAccessOff:     o.Opts.Global.LogLevel != "debug",
	}

	if o.Opts.Global.LogLevel == "debug" || o.Opts.Global.LogLevel == "trace" {
		rc.LogLevel = "debug"
		rc.LogAccessOff = false
	}

	t := template.Must(template.New("local-storage-config").Parse(configYamlV01))
	err := t.Execute(&buff, rc)
	if err != nil {
		return &configuration.Configuration{}, fmt.Errorf("error parsing the config template %v", err)
	}

	config, err := configuration.Parse(bytes.NewReader(buff.Bytes()))
	if err != nil {
		return &configuration.Configuration{}, fmt.Errorf("error parsing local storage configuration : %v", err)
	}
	return config, nil
}

// setupLocalStorage - private function that sets up
// a local (distribution) registry
func (o *ExecutorSchema) setupLocalStorage() error {

	config, err := o.setupLocalRegistryConfig()
	if err != nil {
		o.Log.Error("parsing config %v", err)
	}
	regLogger := logrus.New()
	// prepare the logger
	registryLogPath := filepath.Join(o.LogsDir, registryLogFilename)
	o.registryLogFile, err = os.Create(registryLogPath)
	if err != nil {
		regLogger.Warn("Failed to create log file for local storage registry, using default stderr")
	} else {
		regLogger.Out = o.registryLogFile
	}
	absPath, err := filepath.Abs(registryLogPath)
	o.Log.Debug("local storage registry will log to %s", absPath)
	if err != nil {
		o.Log.Error(err.Error())
	}
	regLogEntry := logrus.NewEntry(regLogger)

	// setup the context
	dcontext.SetDefaultLogger(regLogEntry)
	ctx := dcontext.WithVersion(dcontext.Background(), distversion.Version)
	ctx = dcontext.WithLogger(ctx, regLogEntry)

	errchan := make(chan error)

	reg, err := registry.NewRegistry(ctx, config)
	if err != nil {
		return err
	}

	o.LocalStorageService = *reg
	o.localStorageInterruptChannel = errchan

	go panicOnRegistryError(errchan)
	return nil
}

// startLocalRegistry - private function to start the
// local registry
func startLocalRegistry(reg *registry.Registry, errchan chan error) {
	err := reg.ListenAndServe()
	errchan <- err
}

// panicOnRegistryError - handle errors from local registry
func panicOnRegistryError(errchan chan error) {
	err := <-errchan
	if err != nil && !errors.Is(err, &NormalStorageInterruptError{}) {
		panic(err)
	}
}

// isLocalStoragePortBound - private utility to check if port is bound
func (o *ExecutorSchema) isLocalStoragePortBound() bool {

	// Check if the port is already bound
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", o.Opts.Global.Port))
	if err != nil {
		return true
	}
	listener.Close()
	return false
}

// setupLocalStorageDir - private utility to setup
// the correct local storage directory
func (o *ExecutorSchema) setupLocalStorageDir() error {

	requestedCachePath := os.Getenv(cacheEnvVar)
	if requestedCachePath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		// ensure cache dir exists
		o.LocalStorageDisk = filepath.Join(homeDir, cacheRelativePath)
	} else {
		o.LocalStorageDisk = filepath.Join(requestedCachePath, cacheRelativePath)
	}
	err := os.MkdirAll(o.LocalStorageDisk, 0755)
	if err != nil {
		o.Log.Error("unable to setup folder for oc-mirror local storage: %v ", err)
		return err
	}
	return nil
}

// setupWorkingDir - private utility to setup
// all the relevant working directory structures
func (o *ExecutorSchema) setupWorkingDir() error {
	// ensure working dir exists
	err := o.MakeDir.makeDirAll(o.Opts.Global.WorkingDir, 0755)
	if err != nil {
		o.Log.Error(" setupWorkingDir %v ", err)
		return err
	}

	// create signatures directory
	o.Log.Trace("creating signatures directory %s ", o.Opts.Global.WorkingDir+"/"+signaturesDir)
	err = o.MakeDir.makeDirAll(o.Opts.Global.WorkingDir+"/"+signaturesDir, 0755)
	if err != nil {
		o.Log.Error(" setupWorkingDir %v ", err)
		return err
	}

	// create release-images directory
	o.Log.Trace("creating release images directory %s ", o.Opts.Global.WorkingDir+"/"+releaseImageDir)
	err = o.MakeDir.makeDirAll(o.Opts.Global.WorkingDir+"/"+releaseImageDir, 0755)
	if err != nil {
		o.Log.Error(" setupWorkingDir %v ", err)
		return err
	}

	// create release cache dir
	o.Log.Trace("creating release cache directory %s ", o.Opts.Global.WorkingDir+"/"+releaseImageExtractDir)
	err = o.MakeDir.makeDirAll(o.Opts.Global.WorkingDir+"/"+releaseImageExtractDir, 0755)
	if err != nil {
		o.Log.Error(" setupWorkingDir %v ", err)
		return err
	}

	// create operator cache dir
	o.Log.Trace("creating operator cache directory %s ", o.Opts.Global.WorkingDir+"/"+operatorImageExtractDir)
	err = o.MakeDir.makeDirAll(o.Opts.Global.WorkingDir+"/"+operatorImageExtractDir, 0755)
	if err != nil {
		o.Log.Error(" setupWorkingDir %v ", err)
		return err
	}
	return nil
}

// RunMirrorToDisk - execute the mirror to disk functionality
func (o *ExecutorSchema) RunMirrorToDisk(cmd *cobra.Command, args []string) error {
	startTime := time.Now()

	o.Log.Debug(startMessage, o.Opts.Global.Port)
	go startLocalRegistry(&o.LocalStorageService, o.localStorageInterruptChannel)

	// collect all images
	collectorSchema, err := o.CollectAll(cmd.Context())
	if err != nil {
		return err
	}

	if !o.Opts.IsDryRun {
		// call the batch worker
		err = o.Batch.Worker(cmd.Context(), collectorSchema, *o.Opts)
		if err != nil {
			return err
		}

		// prepare tar.gz when mirror to disk
		// first stop the registry
		interruptSig := NormalStorageInterruptErrorf("end of mirroring to disk. Stopping local storage to prepare the archive")
		o.localStorageInterruptChannel <- interruptSig

		o.Log.Info("📦 Preparing the tarball archive...")
		// next, generate the archive
		err = o.MirrorArchiver.BuildArchive(cmd.Context(), collectorSchema.AllImages)
		if err != nil {
			return err
		}
	} else {
		err = o.DryRun(cmd.Context(), collectorSchema.AllImages)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Info("mirror time     : %v", execTime)

	if err != nil {
		return err
	}
	return nil
}

// RunMirrorToMirror - execute the mirror to mirror functionality
func (o *ExecutorSchema) RunMirrorToMirror(cmd *cobra.Command, args []string) error {
	startTime := time.Now()

	collectorSchema, err := o.CollectAll(cmd.Context())
	if err != nil {
		return err
	}

	// Apply max-nested-paths processing if MaxNestedPaths>0
	if o.Opts.Global.MaxNestedPaths > 0 {
		collectorSchema.AllImages, err = withMaxNestedPaths(collectorSchema.AllImages, o.Opts.Global.MaxNestedPaths)
		if err != nil {
			return err
		}
	}
	if !o.Opts.IsDryRun {
		//call the batch worker
		err = o.Batch.Worker(cmd.Context(), collectorSchema, *o.Opts)
		if err != nil {
			return err
		}

		//create IDMS/ITMS
		forceRepositoryScope := o.Opts.Global.MaxNestedPaths > 0
		err = o.ClusterResources.IDMS_ITMSGenerator(collectorSchema.AllImages, forceRepositoryScope)
		if err != nil {
			return err
		}

		err = o.ClusterResources.CatalogSourceGenerator(collectorSchema.AllImages)
		if err != nil {
			return err
		}

		// create updateService
		if o.Config.Mirror.Platform.Graph {
			graphImage, err := o.Release.GraphImage()
			if err != nil {
				return err
			}
			releaseImage, err := o.Release.ReleaseImage()
			if err != nil {
				return err
			}
			err = o.ClusterResources.UpdateServiceGenerator(graphImage, releaseImage)
			if err != nil {
				return err
			}
		}
	} else {
		err = o.DryRun(cmd.Context(), collectorSchema.AllImages)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Info("mirror time     : %v", execTime)

	return nil
}

// RunDiskToMirror execute the disk to mirror functionality
func (o *ExecutorSchema) RunDiskToMirror(cmd *cobra.Command, args []string) error {
	startTime := time.Now()

	// extract the archive
	err := o.MirrorUnArchiver.Unarchive()
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// start the local storage registry
	o.Log.Debug(startMessage, o.Opts.Global.Port)
	go startLocalRegistry(&o.LocalStorageService, o.localStorageInterruptChannel)

	// collect
	collectorSchema, err := o.CollectAll(cmd.Context())
	if err != nil {
		return err
	}

	// apply max-nested-paths processing if MaxNestedPaths>0
	if o.Opts.Global.MaxNestedPaths > 0 {
		collectorSchema.AllImages, err = withMaxNestedPaths(collectorSchema.AllImages, o.Opts.Global.MaxNestedPaths)
		if err != nil {
			return err
		}
	}

	if !o.Opts.IsDryRun {
		// call the batch worker
		err = o.Batch.Worker(cmd.Context(), collectorSchema, *o.Opts)
		if err != nil {
			return err
		}
		// create IDMS/ITMS
		forceRepositoryScope := o.Opts.Global.MaxNestedPaths > 0
		err = o.ClusterResources.IDMS_ITMSGenerator(collectorSchema.AllImages, forceRepositoryScope)
		if err != nil {
			return err
		}

		// create catalog source
		err = o.ClusterResources.CatalogSourceGenerator(collectorSchema.AllImages)
		if err != nil {
			return err
		}

		// create updateService
		if o.Config.Mirror.Platform.Graph {
			graphImage, err := o.Release.GraphImage()
			if err != nil {
				return err
			}
			releaseImage, err := o.Release.ReleaseImage()
			if err != nil {
				return err
			}
			err = o.ClusterResources.UpdateServiceGenerator(graphImage, releaseImage)
			if err != nil {
				return err
			}
		}
	} else {
		err = o.DryRun(cmd.Context(), collectorSchema.AllImages)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Info("mirror time     : %v", execTime)

	if err != nil {
		return err
	}
	return nil
}

// setupLogsLevelAndDir - private utility to setup log
// level and relevant directory
func (o *ExecutorSchema) setupLogsLevelAndDir() error {
	// override log level
	o.Log.Level(o.Opts.Global.LogLevel)
	// set up location of logs dir
	o.LogsDir = filepath.Join(o.Opts.Global.WorkingDir, logsDir)
	// clean up logs directory
	os.RemoveAll(o.LogsDir)

	// create logs directory
	err := o.MakeDir.makeDirAll(o.LogsDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}
	return nil
}

// CollectAll - collect all relevant images for
// release, operators and additonalImages
func (o *ExecutorSchema) CollectAll(ctx context.Context) (v2alpha1.CollectorSchema, error) {
	startTime := time.Now()

	var collectorSchema v2alpha1.CollectorSchema
	var allRelatedImages []v2alpha1.CopyImageSchema

	o.Log.Info("🕵️  going to discover the necessary images...")
	o.Log.Info("🔍 collecting release images...")
	// collect releases
	rImgs, err := o.Release.ReleaseImageCollector(ctx)
	if err != nil {
		o.closeAll()
		return v2alpha1.CollectorSchema{}, err
	}
	collectorSchema.TotalReleaseImages = len(rImgs)
	o.Log.Debug(collecAllPrefix+"total release images to %s %d ", o.Opts.Function, collectorSchema.TotalReleaseImages)
	o.Opts.ImageType = "release" //TODO ALEX ask to Sherine about it
	allRelatedImages = append(allRelatedImages, rImgs...)

	o.Log.Info("🔍 collecting operator images...")
	// collect operators
	oImgs, err := o.Operator.OperatorImageCollector(ctx)
	if err != nil {
		o.closeAll()
		return v2alpha1.CollectorSchema{}, err
	}
	collectorSchema.TotalOperatorImages = len(oImgs)
	o.Log.Debug(collecAllPrefix+"total operator images to %s %d ", o.Opts.Function, collectorSchema.TotalOperatorImages)
	o.Opts.ImageType = "operator"
	allRelatedImages = append(allRelatedImages, oImgs...)

	o.Log.Info("🔍 collecting additional images...")
	// collect additionalImages
	aImgs, err := o.AdditionalImages.AdditionalImagesCollector(ctx)
	if err != nil {
		o.closeAll()
		return v2alpha1.CollectorSchema{}, err
	}
	collectorSchema.TotalAdditionalImages = len(aImgs)
	o.Log.Debug(collecAllPrefix+"total additional images to %s %d ", o.Opts.Function, collectorSchema.TotalAdditionalImages)
	allRelatedImages = append(allRelatedImages, aImgs...)

	collectorSchema.AllImages = allRelatedImages

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Debug("collection time     : %v", execTime)

	return collectorSchema, nil
}

// closeAll - utility to close any open files
func (o *ExecutorSchema) closeAll() {
	// close registry log file
	err := o.registryLogFile.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error closing log file %s: %v\n", registryLogFilename, err)
	}
}

func withMaxNestedPaths(in []v2alpha1.CopyImageSchema, maxNestedPaths int) ([]v2alpha1.CopyImageSchema, error) {
	out := []v2alpha1.CopyImageSchema{}
	for _, img := range in {
		dst, err := image.WithMaxNestedPaths(img.Destination, maxNestedPaths)
		if err != nil {
			return nil, err
		}
		img.Destination = dst
		out = append(out, img)
	}
	return out, nil
}
