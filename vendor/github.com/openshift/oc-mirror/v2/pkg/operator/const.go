package operator

const (
	indexJson               string = "index.json"
	operatorImageExtractDir string = "hold-operator"
	dockerProtocol          string = "docker://"
	ociProtocol             string = "oci://"
	ociProtocolTrimmed      string = "oci:"
	operatorImageDir        string = "operator-images"
	blobsDir                string = "blobs/sha256" // TODO blobsDir should not make assumptions about algorithm
	errMsg                  string = "[OperatorImageCollector] %v "
	logsFile                string = "operator.log"
)
