package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/google/uuid"

	"github.com/openshift/oc-mirror/v2/internal/pkg/additional"
	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
	"github.com/openshift/oc-mirror/v2/internal/pkg/archive"
	"github.com/openshift/oc-mirror/v2/internal/pkg/batch"
	"github.com/openshift/oc-mirror/v2/internal/pkg/config"
	"github.com/openshift/oc-mirror/v2/internal/pkg/delete"
	"github.com/openshift/oc-mirror/v2/internal/pkg/emoji"
	"github.com/openshift/oc-mirror/v2/internal/pkg/helm"
	clog "github.com/openshift/oc-mirror/v2/internal/pkg/log"
	"github.com/openshift/oc-mirror/v2/internal/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
	"github.com/openshift/oc-mirror/v2/internal/pkg/operator"
	"github.com/openshift/oc-mirror/v2/internal/pkg/release"
	"github.com/spf13/cobra"
)

const (
	deleteErrMsg = "[delete] %v"
	deleteYaml   = "/delete/delete-images.yaml"
	deleteDir    = "/delete/"
)

type DeleteSchema struct {
	ExecutorSchema
	V1Tags bool
}

// NewDeleteCommand - setup all the relevant support structs
// to eventually execute the 'delete' sub command
func NewDeleteCommand(log clog.PluggableLoggerInterface, opts *mirror.CopyOptions) *cobra.Command {
	opts.Function = string(mirror.DeleteMode)

	mkd := MakeDir{}
	ex := &DeleteSchema{
		ExecutorSchema: ExecutorSchema{
			Log:     log,
			Opts:    opts,
			MakeDir: mkd,
		},
	}

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Deletes all related images and manifests from a remote repository or local cache or both",
		Run: func(cmd *cobra.Command, args []string) {
			err := ex.ValidateDelete(args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
			err = ex.CompleteDelete(args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
			defer ex.logFile.Close()
			cmd.SetOutput(ex.logFile)

			// prepare internal storage
			err = ex.setupLocalStorage()
			if err != nil {
				log.Error(" %v ", err)
				os.Exit(1)
			}

			err = ex.RunDelete(cmd)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringVar(&opts.Global.DeleteID, "delete-id", "", "Used to differentiate between versions for files created by the delete functionality")
	cmd.Flags().StringVar(&opts.Global.DeleteYaml, "delete-yaml-file", "", "If set will use the generated or updated yaml file to delete contents")
	cmd.Flags().BoolVar(&opts.Global.ForceCacheDelete, "force-cache-delete", false, "Used to force delete  the local cache manifests and blobs")
	cmd.Flags().Uint16VarP(&opts.Global.Port, "port", "p", 55000, "HTTP port used by oc-mirror's local storage instance")
	cmd.Flags().BoolVar(&opts.Global.DeleteGenerate, "generate", false, "Used to generate the delete yaml for the list of manifests and blobs , used in the step to actually delete from local cahce and remote registry")
	cmd.Flags().BoolVar(&ex.V1Tags, "delete-v1-images", false, "Used during the migration, along with --generate, in order to target images previously mirrored with oc-mirror v1")
	cmd.Flags().UintVar(&ex.ParallelImageLayers, "parallel-layers", 10, "Indicates the number of image layers deleted in parallel. Defaults to 10")
	cmd.Flags().UintVar(&ex.ParallelImages, "parallel-images", 8, "Indicates the number of images deleted in parallel. Defaults to 8")

	// hide flags
	HideFlags(cmd)

	return cmd
}

// Validate - cobra validation
func (o DeleteSchema) ValidateDelete(args []string) error {
	if o.Opts.Global.DeleteGenerate {
		if len(o.Opts.Global.WorkingDir) == 0 {
			return fmt.Errorf("use the --workspace flag, it is mandatory when using the delete command with the --generate flag")
		}
		if !strings.Contains(o.Opts.Global.WorkingDir, fileProtocol) {
			return fmt.Errorf("--workspace flag must have a file:// protocol prefix")
		}
		if len(o.Opts.Global.ConfigPath) == 0 {
			return fmt.Errorf("the --config flag is mandatory when used with the --generate flag")
		}
	} else {
		if len(o.Opts.Global.DeleteYaml) == 0 {
			return fmt.Errorf("the --delete-yaml-file flag is mandatory when not using the --generate flag")
		}
	}
	if o.V1Tags && !o.Opts.Global.DeleteGenerate {
		return fmt.Errorf("the --delete-v1-images flag can only be used alongside the --generate flag")
	}
	if len(args) < 1 {
		return fmt.Errorf("the destination registry is missing in the command arguments")
	}
	if len(args[0]) > 1 && !strings.Contains(args[0], dockerProtocol) {
		return fmt.Errorf("the destination registry argument must have a docker:// protocol prefix")
	}

	deleteFile := o.Opts.Global.DeleteYaml

	_, err := os.Stat(deleteFile)
	if len(o.Opts.Global.DeleteYaml) > 0 && !o.Opts.Global.DeleteGenerate && os.IsNotExist(err) {
		return fmt.Errorf("file not found %s", deleteFile)
	}
	return nil
}

// CompleteDelete - cobra complete
func (o *DeleteSchema) CompleteDelete(args []string) error {
	if args[0] == "" {
		return fmt.Errorf("the destination registry was not found in the command line arguments")
	}
	if !strings.HasPrefix(args[0], dockerProtocol) {
		return fmt.Errorf("the destination registry must be prefixed by docker://")
	}
	o.Opts.Destination = args[0]
	o.Opts.Global.DeleteDestination = args[0]
	if o.Opts.Global.DeleteGenerate {
		o.Log.Debug("delete imagesetconfig file %s ", o.Opts.Global.ConfigPath)
		// read and validate the DeleteImageSetConfiguration
		cfg, err := config.ReadConfig(o.Opts.Global.ConfigPath, v2alpha1.DeleteImageSetConfigurationKind)
		if err != nil {
			return err
		}
		converted := cfg.(v2alpha1.DeleteImageSetConfiguration)
		o.Log.Trace("delete imagesetconfig : %v ", converted)
		if converted.Kind != "DeleteImageSetConfiguration" {
			return fmt.Errorf("using the delete functionality requires the 'DeleteImageSetConfiguration' kind set in the yaml file")
		}
		// we now coerce deleteimagesetconfig to imagesetconfig
		// as we want to use the underlying logic (filtering etc)
		isc := v2alpha1.ImageSetConfiguration{
			ImageSetConfigurationSpec: v2alpha1.ImageSetConfigurationSpec{
				Mirror: v2alpha1.Mirror{
					Platform:         converted.Delete.Platform,
					Operators:        converted.Delete.Operators,
					AdditionalImages: converted.Delete.AdditionalImages,
					Helm:             converted.Delete.Helm,
				},
			},
		}
		o.Config = isc
		o.Opts.RemoveSignatures = true
		// nolint: errcheck
		o.Opts.SrcImage.TlsVerify = false
	}

	o.Opts.Mode = mirror.DiskToMirror

	// update all dependant modules
	mc := mirror.NewMirrorCopy()
	o.Manifest = manifest.New(o.Log)
	o.Mirror = mirror.New(mc, nil)

	// logic to check mode and  WorkingDir
	// always good to check - but this should have been detected in validate
	if o.Opts.Global.DeleteGenerate {
		if strings.Contains(o.Opts.Global.WorkingDir, fileProtocol) {
			wd := strings.Split(o.Opts.Global.WorkingDir, fileProtocol)
			o.Opts.Global.WorkingDir = filepath.Join(wd[1], workingDir)
		} else {
			return fmt.Errorf("--workspace flag must have a file:// protocol prefix")
		}
	}

	// setup logs level, and logsDir under workingDir
	err := o.setupLogsLevelAndDir()
	if err != nil {
		return err
	}
	if o.isLocalStoragePortBound() {
		return fmt.Errorf("%d is already bound and cannot be used", o.Opts.Global.Port)
	}
	o.Opts.LocalStorageFQDN = "localhost:" + strconv.Itoa(int(o.Opts.Global.Port))

	// ensure mirror and batch worker use delete logic
	o.Opts.Function = string(mirror.DeleteMode)
	o.Log.Info(emoji.TwistedRighwardsArrows+" workflow mode: %s / %s", o.Opts.Mode, o.Opts.Function)

	if o.Opts.Global.DeleteGenerate {
		err = o.setupWorkingDir()
		if err != nil {
			return err
		}
		absPath, err := filepath.Abs(o.Opts.Global.WorkingDir + deleteDir)
		if err != nil {
			o.Log.Error("absolute path %v", err)
		}
		if len(o.Opts.Global.DeleteID) > 0 {
			o.Log.Debug("using id %s to update all delete generated files", o.Opts.Global.DeleteID)
		}
		o.Log.Debug("generate flag set, files will be created in %s", absPath)
	}

	if o.Opts.Global.ForceCacheDelete && !o.Opts.Global.DeleteGenerate {
		o.Log.Debug("force-cache-delete flag set, cache deletion will be forced")
	}

	err = o.setupLocalStorageDir()
	if err != nil {
		return err
	}

	client, _ := release.NewOCPClient(uuid.New(), o.Log)
	signature := release.NewSignatureClient(o.Log, o.Config, *o.Opts)
	cn := release.NewCincinnati(o.Log, &o.Config, *o.Opts, client, false, signature)
	o.Release = release.New(o.Log, o.LogsDir, o.Config, *o.Opts, o.Mirror, o.Manifest, cn, o.ImageBuilder)
	o.Batch = batch.New(batch.ChannelConcurrentWorker, o.Log, o.LogsDir, o.Mirror, o.ParallelImages)
	o.Operator = operator.NewWithFilter(o.Log, o.LogsDir, o.Config, *o.Opts, o.Mirror, o.Manifest)

	o.AdditionalImages = additional.New(o.Log, o.Config, *o.Opts, o.Mirror, o.Manifest)
	o.HelmCollector = helm.New(o.Log, o.Config, *o.Opts, nil, nil, &http.Client{Timeout: time.Duration(5) * time.Second})
	if o.V1Tags {
		o.Operator = operator.WithV1Tags(o.Operator)
		o.AdditionalImages = additional.WithV1Tags(o.AdditionalImages)
		o.HelmCollector = helm.WithV1Tags(o.HelmCollector)
	}
	// instantiate delete module
	bg := archive.NewImageBlobGatherer(o.Opts)
	o.Delete = delete.New(o.Log, *o.Opts, o.Batch, bg, o.Config, o.Manifest, o.LocalStorageDisk)

	return nil
}

// RunDelete - cobra run
func (o *DeleteSchema) RunDelete(cmd *cobra.Command) error {
	startTime := time.Now()
	o.Log.Debug("config %v", o.Config)
	o.Log.Debug(startMessage, o.Opts.Global.Port)

	go startLocalRegistry(&o.LocalStorageService, o.localStorageInterruptChannel)

	if o.Opts.Global.DeleteGenerate {

		collectorSchema, err := o.CollectAll(cmd.Context())
		if err != nil {
			return err
		}

		err = o.Delete.WriteDeleteMetaData(collectorSchema.AllImages)
		if err != nil {
			return err
		}
	} else {

		deleteList, err := o.Delete.ReadDeleteMetaData()
		if err != nil {
			return err
		}

		err = o.Delete.DeleteRegistryImages(deleteList)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Info("delete time     : %v", execTime)

	if o.Opts.Global.ForceCacheDelete {
		// finally execute the garbage collector
		// this will delete all relevant blobs
		err := o.startLocalRegistryGarbageCollect()
		if err != nil {
			return err
		}
	}

	if !o.Opts.Global.DeleteGenerate {
		o.Log.Info(emoji.Memo + " Remember to execute a garbage collect (or similar) on your remote repository")
	}
	o.Log.Info(emoji.WavingHandSign + " Goodbye, thank you for using oc-mirror")

	return nil
}

// startLocalRegistryGarbageCollect
func (o *DeleteSchema) startLocalRegistryGarbageCollect() error {
	ctx := context.Background()

	// setup storage driver for garbage-collect
	config, err := o.setupLocalRegistryConfig()
	if err != nil {
		return err
	}
	storageDriver, err := factory.Create(ctx, config.Storage.Type(), config.Storage.Parameters())
	if err != nil {
		return err
	}

	opts := storage.GCOpts{
		DryRun:         false,
		RemoveUntagged: true,
	}

	// used for the garbage-collect
	storageReg, err := storage.NewRegistry(ctx, storageDriver)
	if err != nil {
		return err
	}

	// set up garbage-collect to remove blobs
	err = storage.MarkAndSweep(ctx, storageDriver, storageReg, opts)
	if err != nil {
		return err
	}

	return nil
}
