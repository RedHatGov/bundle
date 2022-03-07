package config

import (
	"errors"
	"fmt"

	"github.com/openshift/oc-mirror/pkg/config/v1alpha2"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

type validationFunc func(cfg *v1alpha2.ImageSetConfiguration) error

var validationChecks = []validationFunc{validateOperatorOptions, validateReleaseChannels}

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

func validateReleaseChannels(cfg *v1alpha2.ImageSetConfiguration) error {
	seen := map[string]bool{}
	for _, channel := range cfg.Mirror.OCP.Channels {
		if seen[channel.Name] {
			return fmt.Errorf(
				"invalid configuration option: duplicate release channel %s found in configuration", channel.Name,
			)
		}
		seen[channel.Name] = true
	}
	return nil
}
