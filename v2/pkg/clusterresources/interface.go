package clusterresources

import (
	"github.com/openshift/oc-mirror/v2/pkg/api/v2alpha1"
)

type GeneratorInterface interface {
	IDMS_ITMSGenerator(allRelatedImages []v2alpha1.CopyImageSchema, forceRepositoryScope bool) error
	UpdateServiceGenerator(graphImage, releaseImage string) error
	CatalogSourceGenerator(allRelatedImages []v2alpha1.CopyImageSchema) error
}
