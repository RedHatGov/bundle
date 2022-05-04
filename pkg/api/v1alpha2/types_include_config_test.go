package v1alpha2

import (
	"testing"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/action"
	"github.com/stretchr/testify/require"
)

func TestConvertToDiffIncludeConfig(t *testing.T) {
	type spec struct {
		name     string
		cfg      IncludeConfig
		exp      action.DiffIncludeConfig
		expError string
	}

	specs := []spec{
		{
			name: "Valid/WithChannels",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						Channels: []IncludeChannel{
							{
								Name: "stable",
								IncludeBundle: IncludeBundle{
									MinVersion: "0.1.0",
									MaxVersion: "0.2.0",
								},
							},
						},
					},
					{
						Name: "foo",
						Channels: []IncludeChannel{
							{
								Name: "stable",
								IncludeBundle: IncludeBundle{
									MinVersion: "0.1.0",
								},
							},
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name: "bar",
						Channels: []action.DiffIncludeChannel{
							{
								Name:  "stable",
								Range: ">=0.1.0 <=0.2.0",
							},
						},
					},
					{
						Name: "foo",
						Channels: []action.DiffIncludeChannel{
							{
								Name: "stable",
								Versions: []semver.Version{
									semver.MustParse("0.1.0"),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Valid/NoChannels",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						IncludeBundle: IncludeBundle{
							MinVersion: "0.1.0",
							MaxVersion: "0.2.0",
						},
					},
					{
						Name: "foo",
						IncludeBundle: IncludeBundle{
							MinVersion: "0.1.0",
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name:  "bar",
						Range: ">=0.1.0 <=0.2.0",
					},
					{
						Name: "foo",
						Versions: []semver.Version{
							semver.MustParse("0.1.0"),
						},
					},
				},
			},
		},
		{
			name: "Valid/WithMinVersionOnly",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						IncludeBundle: IncludeBundle{
							MinVersion: "0.1.0",
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name: "bar",
						Versions: []semver.Version{
							semver.MustParse("0.1.0"),
						},
					},
				},
			},
		},
		{
			name: "Valid/WithMaxVersionOnly",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						IncludeBundle: IncludeBundle{
							MaxVersion: "1.0.0",
						},
					},
					{
						Name: "foo",
						Channels: []IncludeChannel{
							{
								Name: "stable",
								IncludeBundle: IncludeBundle{
									MaxVersion: "0.2.0",
								},
							},
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name:  "bar",
						Range: "<=1.0.0",
					},
					{
						Name: "foo",
						Channels: []action.DiffIncludeChannel{
							{
								Name:  "stable",
								Range: "<=0.2.0",
							},
						},
					},
				},
			},
		},
		{
			name: "Valid/WithMinAndMaxVersion",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						IncludeBundle: IncludeBundle{
							MinVersion: "0.1.0",
							MaxVersion: "0.2.0",
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name:  "bar",
						Range: ">=0.1.0 <=0.2.0",
					},
				},
			},
		},
		{
			name: "Valid/WithMinBundle",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						Name: "bar",
						IncludeBundle: IncludeBundle{
							MinBundle: "bundle-0.1.0",
						},
					},
				},
			},
			exp: action.DiffIncludeConfig{
				Packages: []action.DiffIncludePackage{
					{
						Name: "bar",
						Bundles: []string{
							"bundle-0.1.0",
						},
					},
				},
			},
		},
		{
			name: "Invalid/NoPackageName",
			cfg: IncludeConfig{
				Packages: []IncludePackage{
					{
						IncludeBundle: IncludeBundle{
							MinVersion: "0.1.0",
						},
					},
				},
			},
			exp:      action.DiffIncludeConfig{},
			expError: "package 0 requires a name",
		},
	}

	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			dic, err := s.cfg.ConvertToDiffIncludeConfig()
			if s.expError != "" {
				require.EqualError(t, err, s.expError)
			} else {
				require.NoError(t, err)
				require.Equal(t, s.exp, dic)
			}
		})
	}
}
