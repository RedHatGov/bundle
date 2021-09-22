package create

import (
	"os"

	"github.com/spf13/pflag"

	"github.com/RedHatGov/bundle/pkg/cli"
)

type Options struct {
	*cli.RootOptions

	OutputDir    string
	ConfigPath   string
	SkipImagePin bool
}

func (o *Options) BindFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.ConfigPath, "config", "c", "imageset-config.yaml", "Path to imageset configuration file")
	fs.StringVarP(&o.OutputDir, "output", "o", ".", "output directory for archives")
	fs.BoolVar(&o.SkipImagePin, "skip-image-pin", false, "Do not replace image tags with digest pins in operator catalogs")
}

// ValidatePaths validate the existence of paths from user flags
func (o *Options) ValidatePaths() error {
	if _, err := os.Stat(o.OutputDir); err != nil {
		return err
	}
	if _, err := os.Stat(o.Dir); err != nil {
		return err
	}
	return nil
}
