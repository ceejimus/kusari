package fs

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

func GetRelativePath(fullPath string, relDir string) string {
	if !strings.HasSuffix(relDir, "/") {
		relDir = fmt.Sprintf("%v/", relDir)
	}
	return strings.Replace(fullPath, relDir, "", 1)
}

func FileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	return GetHash(file)
}

func GetHash(r io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
