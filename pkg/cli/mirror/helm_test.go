package mirror

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"k8s.io/client-go/util/jsonpath"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/openshift/oc-mirror/pkg/api/v1alpha2"
)

func TestGetCustomPaths(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  int
	}{
		{
			name:  "tests no additional paths",
			paths: []string{},
			want:  4,
		},
		{
			name: "tests custom paths",
			paths: []string{
				"{.spec.template.spec.tests[*].image}",
			},
			want: 5,
		},
	}
	for _, tt := range tests {

		paths := getImagesPath(tt.paths...)

		if len(paths) != tt.want {
			t.Errorf("in %s, expect to get %d, got %d", tt.name, tt.want, len(paths))
		}
	}

}

func TestSearch(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		ipaths  []string
		want    []v1alpha2.Image
		wantErr bool
	}{
		{
			name:   "test podinfo",
			path:   "testdata/artifacts/podinfo.yaml",
			ipaths: getImagesPath(),
			want: []v1alpha2.Image{
				{Name: "ghcr.io/stefanprodan/podinfo:6.0.0"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {

		b, err := os.ReadFile(tt.path)
		require.NoError(t, err)

		imgs, err := search(b, tt.ipaths...)
		require.NoError(t, err)

		if !reflect.DeepEqual(imgs, tt.want) {
			if !tt.wantErr {
				t.Errorf(`in %s, expect to get "%s", got "%s"`, tt.name, tt.want, imgs)
			}
		}
	}
}

func TestParseJSON(t *testing.T) {

	types := map[string]interface{}{
		"images": "test",
	}

	tests := []struct {
		name     string
		template string
		input    interface{}
		want     []string
		wantErr  bool
	}{
		{
			name:     "tests paths",
			template: "{.images }",
			input:    types,
			want:     []string{"test"},
			wantErr:  false,
		},
		{
			name:     "tests failure",
			template: "{.images }",
			input:    types,
			want:     []string{"nottest"},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		j := jsonpath.New(tt.name)
		j.AllowMissingKeys(true)
		out, err := parseJSONPath(tt.input, j, tt.template)
		require.NoError(t, err)

		if !reflect.DeepEqual(out, tt.want) {
			if !tt.wantErr {
				t.Errorf(`in %s, expect to get "%s", got "%s from %v"`, tt.name, tt.want, out, tt.input)
			}
		}
	}
}

func TestRender(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		ipaths  []string
		wantErr bool
	}{
		{
			name:    "test podinfo",
			path:    "testdata/artifacts/podinfo-6.0.0.tgz",
			wantErr: false,
		},
	}
	for _, tt := range tests {

		chart, err := loader.Load(tt.path)
		require.NoError(t, err)

		result, err := render(chart)
		require.NoError(t, err)

		if _, err = yaml.Marshal(result); err != nil {
			t.Errorf("in %s, render result want not valid %v", tt.name, err)
		}
	}
}

func TestFindImages(t *testing.T) {

	for _, tt := range []struct {
		name    string
		path    string
		want    []v1alpha2.Image
		wantErr bool
	}{
		{
			name: "test podinfo",
			path: "testdata/artifacts/podinfo-6.0.0.tgz",
			want: []v1alpha2.Image{
				{Name: "ghcr.io/stefanprodan/podinfo:6.0.0"},
			},
			wantErr: false,
		},
		{
			name: "chart with aliased subchart",
			path: "testdata/artifacts/my-chart-with-subchart-alias-0.1.0.tgz",
			want: []v1alpha2.Image{
				{Name: "quay.io/rhdh-community/rhdh:next"},
				{Name: "nginx:1.16.0"},
				{Name: "quay.io/fedora/postgresql-15:latest"},
			},
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			imgs, err := findImages(tt.path)

			assertErrorFn := require.NoError
			if tt.wantErr {
				assertErrorFn = require.Error
			}
			assertErrorFn(t, err)

			// Check results ignoring the order in the slices
			assert.ElementsMatchf(t, imgs, tt.want, `in %s, expect to get "%s", got "%s"`, "", tt.want, imgs)
		})
	}
}

func TestIndexFile(t *testing.T) {
	charts, err := IndexFile("https://charts.openshift.io/")
	require.NoError(t, err)
	require.NotNil(t, charts)
}
