package config

import (
	"errors"

	"github.com/openshift/oc-mirror/pkg/config/v1alpha2"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

type validationFunc func(cfg *v1alpha2.ImageSetConfiguration) error

var validationChecks = []validationFunc{validateOperatorOptions, validateReleaseOptions}

func Validate(cfg *v1alpha2.ImageSetConfiguration) error {
	var errs []error
	for _, check := range validationChecks {
		if err := check(cfg); err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func validateOperatorOptions(cfg *v1alpha2.ImageSetConfiguration) error {
	for _, ctlg := range cfg.Mirror.Operators {
		if len(ctlg.IncludeConfig.Packages) != 0 && ctlg.IsHeadsOnly() {
			return errors.New(
				"invalid configuration option: catalog cannot define packages with headsOnly set to true",
			)
		}
	}
	return nil
}

func validateReleaseOptions(cfg *v1alpha2.ImageSetConfiguration) error {
	for _, ch := range cfg.Mirror.OCP.Channels {
		if len(ch.MaxVersion) == 0 && len(ch.MinVersion) > 0 {
			return errors.New(
				"invalid configuration option: release channel must have a maximum version specified",
			)
		}
		if len(ch.MinVersion) == 0 && len(ch.MaxVersion) > 0 {
			return errors.New(
				"invalid configuration option: release channel must have a minimum version specified",
			)
		}
	}
	return nil
}
