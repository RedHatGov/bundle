package batch

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/docker/distribution/registry/api/errcode"
	"github.com/hashicorp/go-multierror"
	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
	clog "github.com/openshift/oc-mirror/v2/internal/pkg/log"
	"github.com/openshift/oc-mirror/v2/internal/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
)

type BatchInterface interface {
	Worker(ctx context.Context, collectorSchema v2alpha1.CollectorSchema, opts mirror.CopyOptions) error
}

func New(log clog.PluggableLoggerInterface,
	logsDir string,
	mirror mirror.MirrorInterface,
	manifest manifest.ManifestInterface,
) BatchInterface {
	return &Batch{Log: log, LogsDir: logsDir, Mirror: mirror, Manifest: manifest}
}

type Batch struct {
	Log      clog.PluggableLoggerInterface
	LogsDir  string
	Mirror   mirror.MirrorInterface
	Manifest manifest.ManifestInterface
}

type BatchSchema struct {
	Writer     io.Writer
	CopyImages []v2alpha1.RelatedImage
	Items      int
	Count      int
	BatchSize  int
	BatchIndex int
	Remainder  int
}

type mirrorErrorSchema struct {
	image v2alpha1.CopyImageSchema
	err   error
}

// Worker - the main batch processor
func (o *Batch) Worker(ctx context.Context, collectorSchema v2alpha1.CollectorSchema, opts mirror.CopyOptions) error {
	startTime := time.Now()

	var mirrorMsg string
	if opts.Function == string(mirror.CopyMode) {
		mirrorMsg = "copying"
	} else if opts.Function == string(mirror.DeleteMode) {
		mirrorMsg = "deleting"
	}

	var errArray []mirrorErrorSchema

	totalImages := len(collectorSchema.AllImages)
	var countTotal, countReleaseImages, countOperatorsImages, countAdditionalImages,
		countErrorTotal, countReleaseImagesErrorTotal, countOperatorsImagesErrorTotal, countAdditionalImagesErrorTotal int

	o.Log.Info("🚀 Start " + mirrorMsg + " the images...")

	for _, img := range collectorSchema.AllImages {
		switch img.Type {
		case v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent, v2alpha1.TypeCincinnatiGraph:
			countReleaseImages++
		case v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorRelatedImage:
			countOperatorsImages++
		case v2alpha1.TypeGeneric:
			countAdditionalImages++
		}

		countTotal++
		var overalProgress string
		if countErrorTotal > 0 {
			overalProgress = fmt.Sprintf("=== Overall Progress - "+mirrorMsg+" image %d / %d (%d errors)===", countTotal, totalImages, countErrorTotal)
		} else {
			overalProgress = fmt.Sprintf("=== Overall Progress - "+mirrorMsg+" image %d / %d ===", countTotal, totalImages)
		}
		o.Log.Info(overalProgress)
		if opts.Function == string(mirror.CopyMode) {
			if countReleaseImagesErrorTotal > 0 {
				o.Log.Info(mirrorMsg+" release image %d / %d (%d errors)", countReleaseImages, collectorSchema.TotalReleaseImages, countReleaseImagesErrorTotal)
			} else {
				o.Log.Info(mirrorMsg+" release image %d / %d", countReleaseImages, collectorSchema.TotalReleaseImages)
			}
			if countOperatorsImagesErrorTotal > 0 {
				o.Log.Info(mirrorMsg+" operator image %d / %d (%d errors)", countOperatorsImages, collectorSchema.TotalOperatorImages, countOperatorsImagesErrorTotal)
			} else {
				o.Log.Info(mirrorMsg+" operator image %d / %d", countOperatorsImages, collectorSchema.TotalOperatorImages)
			}
			if countAdditionalImagesErrorTotal > 0 {
				o.Log.Info(mirrorMsg+" additional image %d / %d (%d errors)", countAdditionalImages, collectorSchema.TotalAdditionalImages, countAdditionalImagesErrorTotal)
			} else {
				o.Log.Info(mirrorMsg+" additional image %d / %d", countAdditionalImages, collectorSchema.TotalAdditionalImages)
			}
			o.Log.Info(strings.Repeat("=", len(overalProgress)))
		}

		o.Log.Debug(mirrorMsg+" image: %s", img.Origin)

		if img.Type == v2alpha1.TypeCincinnatiGraph && (opts.Mode == mirror.MirrorToDisk || opts.Mode == mirror.MirrorToMirror) {
			continue
		}

		err := o.Mirror.Run(ctx, img.Source, img.Destination, mirror.Mode(opts.Function), &opts)
		if err != nil && !isFailSafe(err) {
			return err
		}

		if err != nil {
			errArray = append(errArray, mirrorErrorSchema{image: img, err: err})
			countErrorTotal++
			switch img.Type {
			case v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent, v2alpha1.TypeCincinnatiGraph:
				countReleaseImagesErrorTotal++
			case v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorRelatedImage:
				countOperatorsImagesErrorTotal++
			case v2alpha1.TypeGeneric:
				countAdditionalImagesErrorTotal++
			}
		}
	}

	if opts.Function == string(mirror.CopyMode) {
		o.Log.Info("=== Results ===")
		if countReleaseImages == collectorSchema.TotalReleaseImages && countReleaseImagesErrorTotal == 0 {
			o.Log.Info("All release images mirrored successfully %d / %d ✅", countReleaseImages, collectorSchema.TotalReleaseImages)
		} else {
			o.Log.Info("Images mirrored %d / %d: Some release images failed to mirror ❌ - please check the logs", countReleaseImages-countReleaseImagesErrorTotal, collectorSchema.TotalReleaseImages)
		}

		if countOperatorsImages == collectorSchema.TotalOperatorImages && countOperatorsImagesErrorTotal == 0 {
			o.Log.Info("All operator images mirrored successfully %d / %d ✅", countOperatorsImages, collectorSchema.TotalOperatorImages)
		} else {
			o.Log.Info("Images mirrored %d / %d: Some operator images failed to mirror ❌ - please check the logs", countOperatorsImages-countOperatorsImagesErrorTotal, collectorSchema.TotalOperatorImages)
		}

		if countAdditionalImages == collectorSchema.TotalAdditionalImages && countAdditionalImagesErrorTotal == 0 {
			o.Log.Info("All additional images mirrored successfully %d / %d ✅", countAdditionalImages, collectorSchema.TotalAdditionalImages)
		} else {
			o.Log.Info("Images mirrored %d / %d: Some additional images failed to mirror ❌ - please check the logs", countAdditionalImages-countAdditionalImagesErrorTotal, collectorSchema.TotalAdditionalImages)
		}
	} else {
		o.Log.Info("=== Results ===")
		if countTotal == totalImages && countErrorTotal == 0 {
			o.Log.Info("All images deleted successfully %d / %d ✅", countTotal, totalImages)
		} else {
			o.Log.Info("Images deleted %d / %d: Some images failed to delete ❌ - please check the logs", countTotal-countErrorTotal, totalImages)
		}
	}

	if len(errArray) > 0 {
		timestamp := time.Now().Format("20060102_150405")
		filename := fmt.Sprintf("mirroring_errors_%s.txt", timestamp)

		file, err := os.Create(filepath.Join(o.LogsDir, filename))
		if err != nil {
			o.Log.Error(workerPrefix+"failed to create file: %s", err.Error())
			return err
		}
		defer file.Close()

		for _, err := range errArray {
			errorMsg := fmt.Sprintf("error mirroring image %s error: %s", err.image.Origin, err.err.Error())
			o.Log.Error(workerPrefix + errorMsg)
			fmt.Fprintln(file, errorMsg)
		}

		return fmt.Errorf(workerPrefix+"some errors happened during the mirroring - refer to %s for more details", o.LogsDir+"/"+filename)
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Debug("batch time     : %v", execTime)
	return nil
}

func isFailSafe(err error) bool {
	switch err {
	case nil:
		return false
	case context.Canceled, context.DeadlineExceeded:
		return false
	default: // continue
	}

	type unwrapper interface {
		Unwrap() error
	}

	switch e := err.(type) {

	case errcode.Error:
		switch e.Code {
		case errcode.ErrorCodeUnauthorized, errcode.ErrorCodeDenied:
			return false
		}
		return true
	case *net.OpError:
		return isFailSafe(e.Err)
	case *url.Error: // This includes errors returned by the net/http client.
		if e.Err == io.EOF { // Happens when a server accepts a HTTP connection and sends EOF
			return true
		}
		return isFailSafe(e.Err)
	case syscall.Errno:
		return isErrnoRetryable(e)
	case errcode.Errors:
		// if this error is a group of errors, process them all in turn
		for i := range e {
			if !isFailSafe(e[i]) {
				return false
			}
		}
		return true
	case *multierror.Error:
		// if this error is a group of errors, process them all in turn
		for i := range e.Errors {
			if !isFailSafe(e.Errors[i]) {
				return false
			}
		}
		return true
	case net.Error:
		if e.Timeout() {
			return true
		}
		if unwrappable, ok := e.(unwrapper); ok {
			err = unwrappable.Unwrap()
			return isFailSafe(err)
		}
	case unwrapper: // Test this last, because various error types might implement .Unwrap()
		err = e.Unwrap()
		return isFailSafe(err)
	}

	return false
}

func isErrnoRetryable(e error) bool {
	switch e {
	case syscall.ECONNREFUSED, syscall.EINTR, syscall.EAGAIN, syscall.EBUSY, syscall.ENETDOWN, syscall.ENETUNREACH, syscall.ENETRESET, syscall.ECONNABORTED, syscall.ECONNRESET, syscall.ETIMEDOUT, syscall.EHOSTDOWN, syscall.EHOSTUNREACH:
		return true
	}
	return isErrnoERESTART(e)
}

func isErrnoERESTART(e error) bool {
	return e == syscall.ERESTART
}
