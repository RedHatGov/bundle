package release

const (
	graphBaseImage                = "registry.access.redhat.com/ubi9/ubi:latest"
	graphURL                      = "https://api.openshift.com/api/upgrades_info/graph-data"
	graphArchive                  = "cincinnati-graph-data.tar"
	graphPreparationDir           = "graph-preparation"
	buildGraphDataDir             = "/var/lib/cincinnati-graph-data"
	graphDataMountPath            = "/var/lib/cincinnati/graph-data"
	graphImageName                = "openshift/graph-image"
	indexJson                     = "manifest.json"
	operatorImageExtractDir       = "hold-operator"
	workingDir                    = "working-dir"
	dockerProtocol                = "docker://"
	ociProtocol                   = "oci://"
	ociProtocolTrimmed            = "oci:"
	dirProtocol                   = "dir://"
	dirProtocolTrimmed            = "dir:"
	releaseImageDir               = "release-images"
	releaseIndex                  = "release-index"
	operatorImageDir              = "operator-images"
	cincinnatiGraphDataDir        = "cincinnati-graph-data"
	releaseImageExtractDir        = "hold-release"
	releaseManifests              = "release-manifests"
	releaseBootableImages         = "0000_50_installer_coreos-bootimages.yaml"
	releaseBootableImagesFullPath = releaseManifests + "/" + releaseBootableImages
	imageReferences               = "image-references"
	releaseImageExtractFullPath   = releaseManifests + "/" + imageReferences
	blobsDir                      = "blobs/sha256"
	collectorPrefix               = "[ReleaseImageCollector] "
	errMsg                        = collectorPrefix + "%s"
	logFile                       = "release.log"
)
