package archive

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type MirrorUnArchiver struct {
	UnArchiver
	workingDir         string
	cacheDir           string
	archiveFile        *os.File
	hasNoFilesToUnpack bool
}

func NewArchiveExtractor(archivePath, workingDir, cacheDir string) (MirrorUnArchiver, error) {
	chunk := 1
	archiveFileName := fmt.Sprintf("%s_%06d.tar", archiveFilePrefix, chunk)
	chunkPath := filepath.Join(archivePath, archiveFileName)

	hasNoFilesToUnpack := false

	chunkFile, err := os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			hasNoFilesToUnpack = true
		} else {
			return MirrorUnArchiver{}, err
		}
	}

	ae := MirrorUnArchiver{
		workingDir:         workingDir,
		cacheDir:           cacheDir,
		archiveFile:        chunkFile,
		hasNoFilesToUnpack: hasNoFilesToUnpack,
	}
	return ae, nil
}

func (o MirrorUnArchiver) Close() error {
	return o.archiveFile.Close()
}

// Unarchive extracts:
// * docker/v2* to cacheDir
// * working-dir to workingDir
func (o MirrorUnArchiver) Unarchive() error {
	if !o.hasNoFilesToUnpack {
		reader := tar.NewReader(o.archiveFile)
		// make sure workingDir exists
		err := os.MkdirAll(o.workingDir, 0755)
		if err != nil {
			return fmt.Errorf(errMessageFolder, o.workingDir, err)
		}
		// make sure cacheDir exists
		err = os.MkdirAll(o.cacheDir, 0755)
		if err != nil {
			return fmt.Errorf(errMessageFolder, o.cacheDir, err)
		}
		for {
			header, err := reader.Next()

			// break the infinite loop when EOF
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return fmt.Errorf("error reading archive %s: %v", o.archiveFile.Name(), err)
			}

			if header == nil {
				continue
			}
			// taking only files into account
			// because we are considering that all parent folders will be
			// created recursively, and that, to the best of our knowledge
			// the archive doesn't include any symbolic links

			// for the moment we ignore imageSetConfig that is
			// included in the tar
			// as well as any other files that are not
			// working-dir or cache

			if header.Typeflag == tar.TypeReg {
				descriptor := ""
				// case file belongs to working-dir
				if strings.Contains(header.Name, workingDirectory) {
					workingDirParent := filepath.Dir(o.workingDir)
					descriptor = filepath.Join(workingDirParent, header.Name)
				} else if strings.Contains(header.Name, cacheFilePrefix) {
					// case file belongs to the cache
					descriptor = filepath.Join(o.cacheDir, header.Name)
				} else {
					continue
				}
				// make sure all the parent directories exist
				descriptorParent := filepath.Dir(descriptor)
				if err := os.MkdirAll(descriptorParent, 0755); err != nil {
					return fmt.Errorf(errMessageFolder, descriptorParent, err)
				}
				// if it's a file create it, making sure it's at least writable and executable by the user
				// since with every UnArchive, we should be able to rewrite the file
				f, err := os.OpenFile(descriptor, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode)|0755)
				if err != nil {
					return fmt.Errorf("unable to create file %s: %v", descriptor, err)
				}
				// copy  contents
				if _, err := io.Copy(f, reader); err != nil {
					return fmt.Errorf("error copying file %s: %v", descriptor, err)
				}

				// manually close here after each file operation; defering would cause each file close
				// to wait until all operations have completed.
				f.Close()

			}
		}
		return nil
	}
	return nil
}
