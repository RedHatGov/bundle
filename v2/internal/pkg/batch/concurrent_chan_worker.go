package batch

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/openshift/oc-mirror/v2/internal/pkg/api/v2alpha1"
	"github.com/openshift/oc-mirror/v2/internal/pkg/emoji"
	clog "github.com/openshift/oc-mirror/v2/internal/pkg/log"
	"github.com/openshift/oc-mirror/v2/internal/pkg/mirror"
	"github.com/openshift/oc-mirror/v2/internal/pkg/spinners"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

var (
	copiedImages v2alpha1.CollectorSchema

	errMsgHeader = "%ssome errors occurred during the mirroring"
	errMsg       = errMsgHeader + ".\n" +
		"\t Please review %s/%s for a list of mirroring errors.\n" +
		"\t You may consider:\n" +
		"\t * removing images or operators that cause the error from the image set config, and retrying\n" +
		"\t * keeping the image set config (images are mandatory for you), and retrying\n" +
		"\t * mirroring the failing images manually, if retries also fail."
)

type ChannelConcurrentBatch struct {
	Log           clog.PluggableLoggerInterface
	LogsDir       string
	Mirror        mirror.MirrorInterface
	MaxGoroutines int
}

type GoroutineResult struct {
	err     *mirrorErrorSchema
	imgType v2alpha1.ImageType
	img     v2alpha1.CopyImageSchema
}

// Worker - the main batch processor
func (o *ChannelConcurrentBatch) Worker(ctx context.Context, collectorSchema v2alpha1.CollectorSchema, opts mirror.CopyOptions) (v2alpha1.CollectorSchema, error) {
	startTime := time.Now()

	copiedImages = v2alpha1.CollectorSchema{
		AllImages: []v2alpha1.CopyImageSchema{},
	}

	var errArray []mirrorErrorSchema

	var m sync.RWMutex
	var wg sync.WaitGroup

	var mirrorMsg string
	switch {
	case opts.IsCopy():
		mirrorMsg = "copying"
	case opts.IsDelete():
		mirrorMsg = "deleting"
	}

	opts.PreserveDigests = true

	o.Log.Info(emoji.Rocket + " Start " + mirrorMsg + " the images...")
	o.Log.Info(emoji.Pushpin+" images to %s %d ", opts.Function, len(collectorSchema.AllImages))

	total := len(collectorSchema.AllImages)
	p := mpb.New(mpb.PopCompletedMode(), mpb.ContainerOptional(mpb.WithOutput(io.Discard), !opts.Global.IsTerminal))
	results := make(chan GoroutineResult, total)
	progressCh := make(chan int, total)
	semaphore := make(chan struct{}, o.MaxGoroutines)

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer close(results)
		defer close(semaphore)

		for _, img := range collectorSchema.AllImages {

			select {
			case <-cancelCtx.Done():
				wg.Wait()
				return
			default:
			}

			if !opts.Global.IsTerminal {
				var dest string
				if strings.Contains(img.Destination, opts.LocalStorageFQDN) {
					dest = "cache"
				} else {
					dest = hostNamespace(img.Destination)
				}
				o.Log.Debug("Batch %s %s to %s", mirrorMsg, path.Base(img.Origin), dest)
			}
			sp := newSpinner(img, opts.LocalStorageFQDN, p)

			semaphore <- struct{}{}
			wg.Add(1)
			go func(cancelCtx context.Context, semaphore chan struct{}, results chan<- GoroutineResult, spinner *mpb.Bar) {
				defer wg.Done()
				defer func() { <-semaphore }()
				result := GoroutineResult{imgType: img.Type, img: img}

				m.Lock()
				skip, reason := shouldSkipImage(img, opts, errArray)
				m.Unlock()
				if skip {
					if reason != nil {
						result.err = &mirrorErrorSchema{image: img, err: reason}
					}

					switch img.Type {
					case v2alpha1.TypeOperatorBundle:
						spinner.Abort(false)
					case v2alpha1.TypeCincinnatiGraph:
						spinner.Increment()
					}

					results <- result
					return
				}

				var err error
				var triggered bool
			loop:
				for {
					select {
					case <-cancelCtx.Done():
						spinner.Abort(false)
						break loop
					default:
						if !triggered {
							triggered = true
							timeoutCtx, _ := opts.Global.CommandTimeoutContext()

							err = o.Mirror.Run(timeoutCtx, img.Source, img.Destination, mirror.Mode(opts.Function), &opts)

							switch {
							case err == nil:
								spinner.Increment()
								if !opts.Global.IsTerminal {
									o.Log.Info("Success %s %s", mirrorMsg, img.Origin)
								}
							case img.Type.IsOperator():
								operators := collectorSchema.CopyImageSchemaMap.OperatorsByImage[img.Origin]
								bundles := collectorSchema.CopyImageSchemaMap.BundlesByImage[img.Origin]
								result.err = &mirrorErrorSchema{image: img, err: err, operators: operators, bundles: bundles}
								spinner.Abort(false)
								if !opts.Global.IsTerminal {
									o.Log.Error("Failed %s operator %s", mirrorMsg, img.Origin)
								}
							case img.Type.IsRelease() || img.Type.IsAdditionalImage() || img.Type.IsHelmImage():
								result.err = &mirrorErrorSchema{image: img, err: err}
								spinner.Abort(false)
								if !opts.Global.IsTerminal {
									o.Log.Error("Failed %s image %s", mirrorMsg, img.Origin)
								}
							}
							results <- result
							break loop
						}

					}
				}
			}(cancelCtx, semaphore, results, sp)
		}
		wg.Wait()
	}()

	overallProgress := newOverallProgress(p, total)

	go runOverallProgress(overallProgress, cancelCtx, progressCh)

	completed := 0
	for completed < len(collectorSchema.AllImages) {
		res := <-results
		err := res.err
		if err == nil {
			copiedImages.AllImages = append(copiedImages.AllImages, res.img)
			incrementTotals(res.imgType, &copiedImages)
		} else {
			m.Lock()
			errArray = append(errArray, *err)
			m.Unlock()

			if res.imgType.IsRelease() {
				cancel()
				break
			}
		}

		completed++
		progressCh <- 1
	}
	close(progressCh)

	p.Wait()

	logResults(o.Log, opts.Function, &copiedImages, &collectorSchema)

	if len(errArray) > 0 {
		filename, err := saveErrors(o.Log, o.LogsDir, errArray)
		if err != nil {
			return copiedImages, NewSafeError(errMsgHeader+" - unable to log these errors in %s/%s: %s", workerPrefix, o.LogsDir, filename, err.Error())
		} else {
			return copiedImages, NewSafeError(errMsg, workerPrefix, o.LogsDir, filename)
		}
	}
	endTime := time.Now()
	execTime := endTime.Sub(startTime)
	o.Log.Debug("concurrent channel worker time     : %v", execTime)
	return collectorSchema, nil
}

func hostNamespace(input string) string {
	parsedURL, err := url.Parse(input)
	if err != nil {
		return ""
	}

	host := parsedURL.Host
	pathSegments := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")

	if len(pathSegments) > 1 {
		namespacePath := strings.Join(pathSegments[:len(pathSegments)-1], "/")
		hostAndNamespace := path.Join(host, namespacePath) + "/"
		return hostAndNamespace
	} else if len(pathSegments) == 1 {
		return path.Join(host, pathSegments[0]) + "/"
	} else {
		return host
	}
}

func logResults(log clog.PluggableLoggerInterface, copyMode string, copiedImages, collectorSchema *v2alpha1.CollectorSchema) {
	log.Info("=== Results ===")

	var copyModeMsg string
	if copyMode == string(mirror.CopyMode) {
		copyModeMsg = "mirrored"
	} else {
		copyModeMsg = "deleted"
	}

	logResult(log, copyModeMsg, "release", copiedImages.TotalReleaseImages, collectorSchema.TotalReleaseImages)
	logResult(log, copyModeMsg, "operator", copiedImages.TotalOperatorImages, collectorSchema.TotalOperatorImages)
	logResult(log, copyModeMsg, "additional", copiedImages.TotalAdditionalImages, collectorSchema.TotalAdditionalImages)
	logResult(log, copyModeMsg, "helm", copiedImages.TotalHelmImages, collectorSchema.TotalHelmImages)
}

func logResult(log clog.PluggableLoggerInterface, copyMode, imageType string, copied, total int) {
	if total != 0 {
		if copied == total {
			log.Info(emoji.SpinnerCheckMark+" %d / %d %s images %s successfully", copied, total, imageType, copyMode)
		} else {
			log.Info(emoji.SpinnerCrossMark+" %d / %d %s images %s: Some %s images failed to be %s - please check the logs", copied, total, imageType, copyMode, imageType, copyMode)
		}
	}
}

func newSpinner(img v2alpha1.CopyImageSchema, localStorage string, p *mpb.Progress) *mpb.Bar {
	imageText := " " + path.Base(img.Origin) + " "
	if strings.Contains(img.Destination, localStorage) {
		imageText += emoji.RightArrow + "  cache "
	} else {
		imageText += emoji.RightArrow + "  " + hostNamespace(img.Destination) + " "
	}

	return p.AddSpinner(
		1,
		mpb.BarFillerMiddleware(spinners.PositionSpinnerLeft),
		mpb.BarWidth(3),
		mpb.PrependDecorators(
			decor.OnComplete(spinners.EmptyDecorator(), emoji.SpinnerCheckMark),
			decor.OnAbort(spinners.EmptyDecorator(), emoji.SpinnerCrossMark),
		),
		mpb.AppendDecorators(
			decor.Name("("),
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(")"),
			decor.Name(imageText),
		),
		mpb.BarFillerClearOnComplete(),
		spinners.BarFillerClearOnAbort(),
	)
}

func newOverallProgress(p *mpb.Progress, total int) *mpb.Bar {
	return p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.CountersNoUnit("%d / %d"),
			decor.Name(" ("),
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(")"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
		mpb.BarPriority(total+1),
	)
}

func runOverallProgress(overallProgress *mpb.Bar, cancelCtx context.Context, progressCh chan int) {
	var progress int

	for {
		select {
		case <-cancelCtx.Done():
			overallProgress.Abort(false)
			return
		case <-progressCh:
			progress++
			overallProgress.SetCurrent(int64(progress))
		}
	}

}

func incrementTotals(imgType v2alpha1.ImageType, copiedImages *v2alpha1.CollectorSchema) {
	switch imgType {
	case v2alpha1.TypeCincinnatiGraph, v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent:
		copiedImages.TotalReleaseImages++
	case v2alpha1.TypeGeneric:
		copiedImages.TotalAdditionalImages++
	case v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorRelatedImage:
		copiedImages.TotalOperatorImages++
	case v2alpha1.TypeHelmImage:
		copiedImages.TotalHelmImages++
	}
}

// shouldSkipImage helps determine whether the batch should perform the mirroring of the image
// or if the image should be skipped.
func shouldSkipImage(img v2alpha1.CopyImageSchema, opts mirror.CopyOptions, errArray []mirrorErrorSchema) (bool, error) {
	// In MirrorToMirror and MirrorToDisk, the release collector will generally build and push the graph image
	// to the destination registry (disconnected registry or cache resp.)
	// Therefore this image can be skipped.
	// OCPBUGS-38037: The only exception to this is in the enclave environment. Enclave environment is detected by the presence
	// of env var UPDATE_URL_OVERRIDE.
	// When in enclave environment, release collector cannot build nor push the graph image. Therefore graph image
	// should not be skipped.
	updateURLOverride := os.Getenv("UPDATE_URL_OVERRIDE")
	if img.Type == v2alpha1.TypeCincinnatiGraph && (opts.Mode == mirror.MirrorToDisk || opts.Mode == mirror.MirrorToMirror) && len(updateURLOverride) == 0 {
		return true, nil
	}

	if img.Type == v2alpha1.TypeOperatorBundle {
		for _, err := range errArray {
			bundleImage := img.Origin
			if strings.Contains(bundleImage, "://") {
				bundleImage = strings.Split(img.Origin, "://")[1]
			}

			if err.bundles != nil && err.bundles.Has(bundleImage) {
				return true, fmt.Errorf(skippingMsg, img.Origin)
			}
		}
	}

	return false, nil
}
