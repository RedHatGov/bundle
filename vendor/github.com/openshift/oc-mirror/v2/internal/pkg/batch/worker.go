package batch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
	clog "github.com/openshift/oc-mirror/v2/internal/pkg/log"
	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
)

type BatchInterface interface {
	Worker(ctx context.Context, collectorSchema v2alpha1.CollectorSchema, opts mirror.CopyOptions) (v2alpha1.CollectorSchema, error)
}

func New(log clog.PluggableLoggerInterface,
	logsDir string,
	mirror mirror.MirrorInterface,
) BatchInterface {
	copiedImages := v2alpha1.CollectorSchema{
		AllImages: []v2alpha1.CopyImageSchema{},
	}
	return &Batch{Log: log, LogsDir: logsDir, Mirror: mirror, CopiedImages: copiedImages}
}

type Batch struct {
	Log          clog.PluggableLoggerInterface
	LogsDir      string
	Mirror       mirror.MirrorInterface
	CopiedImages v2alpha1.CollectorSchema
}

type mirrorErrorSchema struct {
	image v2alpha1.CopyImageSchema
	err   error
}

// Worker - the main batch processor
func (o *Batch) Worker(ctx context.Context, collectorSchema v2alpha1.CollectorSchema, opts mirror.CopyOptions) (v2alpha1.CollectorSchema, error) {
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
		o.Log.Info(mirrorMsg+" image: %s", img.Origin)

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
			if collectorSchema.TotalReleaseImages > 0 {
				if countReleaseImagesErrorTotal > 0 {
					o.Log.Info(mirrorMsg+" release image %d / %d (%d errors)", countReleaseImages, collectorSchema.TotalReleaseImages, countReleaseImagesErrorTotal)
				} else {
					o.Log.Info(mirrorMsg+" release image %d / %d", countReleaseImages, collectorSchema.TotalReleaseImages)
				}
			}
			if collectorSchema.TotalOperatorImages > 0 {
				if countOperatorsImagesErrorTotal > 0 {
					o.Log.Info(mirrorMsg+" operator image %d / %d (%d errors)", countOperatorsImages, collectorSchema.TotalOperatorImages, countOperatorsImagesErrorTotal)
				} else {
					o.Log.Info(mirrorMsg+" operator image %d / %d", countOperatorsImages, collectorSchema.TotalOperatorImages)
				}
			}
			if collectorSchema.TotalAdditionalImages > 0 {
				if countAdditionalImagesErrorTotal > 0 {
					o.Log.Info(mirrorMsg+" additional image %d / %d (%d errors)", countAdditionalImages, collectorSchema.TotalAdditionalImages, countAdditionalImagesErrorTotal)
				} else {
					o.Log.Info(mirrorMsg+" additional image %d / %d", countAdditionalImages, collectorSchema.TotalAdditionalImages)
				}
			}
			o.Log.Info(strings.Repeat("=", len(overalProgress)))
		}

		if img.Type == v2alpha1.TypeCincinnatiGraph && (opts.Mode == mirror.MirrorToDisk || opts.Mode == mirror.MirrorToMirror) {
			continue
		}

		err := o.Mirror.Run(ctx, img.Source, img.Destination, mirror.Mode(opts.Function), &opts)
		isSafe := isFailSafe(err)
		switch {
		case err == nil:
			o.CopiedImages.AllImages = append(o.CopiedImages.AllImages, img)
			// switch img.Type {
			// case v2alpha1.TypeCincinnatiGraph, v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent:
			// 	o.CopiedImages.TotalReleaseImages++
			// case v2alpha1.TypeGeneric:
			// 	o.CopiedImages.TotalAdditionalImages++
			// case v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorRelatedImage:
			// 	o.CopiedImages.TotalOperatorImages++
			// }
		case err != nil && isSafe && img.Type != v2alpha1.TypeOCPRelease && img.Type != v2alpha1.TypeOCPReleaseContent:
			// this error is fail safe, we're continuing to mirror other images.
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
		case err != nil && isSafe && (img.Type == v2alpha1.TypeOCPRelease || img.Type == v2alpha1.TypeOCPReleaseContent):
			fallthrough
		case err != nil && !isSafe:
			// this error is fail fast, we save the errArray and immediately return `UnsafeError` to caller
			currentMirrorError := mirrorErrorSchema{image: img, err: err}
			errArray = append(errArray, currentMirrorError)
			filename, saveError := o.saveErrors(errArray)
			if saveError != nil {
				o.Log.Error("unable to log these errors in %s: %v", o.LogsDir+"/"+filename, saveError)
			}
			return o.CopiedImages, NewUnsafeError(currentMirrorError)
		}
	}

	if opts.Function == string(mirror.CopyMode) {
		o.Log.Info("=== Results ===")
		if collectorSchema.TotalReleaseImages != 0 {
			if countReleaseImages == collectorSchema.TotalReleaseImages && countReleaseImagesErrorTotal == 0 {
				o.Log.Info("All release images mirrored successfully %d / %d ✅", countReleaseImages, collectorSchema.TotalReleaseImages)
			} else {
				o.Log.Info("Images mirrored %d / %d: Some release images failed to mirror ❌ - please check the logs", countReleaseImages-countReleaseImagesErrorTotal, collectorSchema.TotalReleaseImages)
			}
		}
		if collectorSchema.TotalOperatorImages != 0 {
			if countOperatorsImages == collectorSchema.TotalOperatorImages && countOperatorsImagesErrorTotal == 0 {
				o.Log.Info("All operator images mirrored successfully %d / %d ✅", countOperatorsImages, collectorSchema.TotalOperatorImages)
			} else {
				o.Log.Info("Images mirrored %d / %d: Some operator images failed to mirror ❌ - please check the logs", countOperatorsImages-countOperatorsImagesErrorTotal, collectorSchema.TotalOperatorImages)
			}
		}
		if collectorSchema.TotalAdditionalImages != 0 {
			if countAdditionalImages == collectorSchema.TotalAdditionalImages && countAdditionalImagesErrorTotal == 0 {
				o.Log.Info("All additional images mirrored successfully %d / %d ✅", countAdditionalImages, collectorSchema.TotalAdditionalImages)
			} else {
				o.Log.Info("Images mirrored %d / %d: Some additional images failed to mirror ❌ - please check the logs", countAdditionalImages-countAdditionalImagesErrorTotal, collectorSchema.TotalAdditionalImages)
			}
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
		filename, err := o.saveErrors(errArray)
		if err != nil {
			return o.CopiedImages, NewSafeError(workerPrefix+"some errors occurred during the mirroring - unable to log these errors in %s: %v", o.LogsDir+"/"+filename, err)
		} else {
			return o.CopiedImages, NewSafeError(workerPrefix+"some errors occurred during the mirroring - refer to %s for more details", o.LogsDir+"/"+filename)
		}
	}

	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Debug("batch time     : %v", execTime)
	return o.CopiedImages, nil
}

func (o *Batch) saveErrors(errArray []mirrorErrorSchema) (string, error) {
	if len(errArray) > 0 {
		timestamp := time.Now().Format("20060102_150405")
		filename := fmt.Sprintf("mirroring_errors_%s.txt", timestamp)
		file, err := os.Create(filepath.Join(o.LogsDir, filename))
		if err != nil {
			o.Log.Error(workerPrefix+"failed to create file: %s", err.Error())
			return filename, err
		}
		defer file.Close()

		for _, err := range errArray {
			errorMsg := fmt.Sprintf("error mirroring image %s error: %s", err.image.Origin, err.err.Error())
			o.Log.Error(workerPrefix + errorMsg)
			fmt.Fprintln(file, errorMsg)
		}
		return filename, nil
	}
	return "", nil
}
