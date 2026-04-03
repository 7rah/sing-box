//go:build darwin || linux

package libbox

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/service/oomkiller"
)

func init() {
	sOOMReporter = &oomReporter{}
}

var oomReportProfiles = []string{
	"allocs",
	"block",
	"goroutine",
	"heap",
	"mutex",
	"threadcreate",
}

type oomReportMetadata struct {
	Source              string `json:"source,omitempty"`
	BundleIdentifier    string `json:"bundleIdentifier,omitempty"`
	ProcessName         string `json:"processName,omitempty"`
	ProcessPath         string `json:"processPath,omitempty"`
	StartedAt           string `json:"startedAt,omitempty"`
	RecordedAt          string `json:"recordedAt"`
	AppVersion          string `json:"appVersion,omitempty"`
	AppMarketingVersion string `json:"appMarketingVersion,omitempty"`
	CoreVersion         string `json:"coreVersion,omitempty"`
	GoVersion           string `json:"goVersion,omitempty"`
	MemoryUsage         uint64 `json:"memoryUsage"`
}

type oomReporter struct{}

var _ oomkiller.OOMReporter = (*oomReporter)(nil)

func (r *oomReporter) WriteReport(memoryUsage uint64) error {
	now := time.Now().UTC()
	reportsDir := filepath.Join(sWorkingPath, "oom_reports")
	os.MkdirAll(reportsDir, 0o777)

	destPath := nextAvailableReportPath(reportsDir, now)
	os.MkdirAll(destPath, 0o777)
	if runtime.GOOS != "android" {
		os.Chown(reportsDir, sUserID, sGroupID)
		os.Chown(destPath, sUserID, sGroupID)
	}

	for _, name := range oomReportProfiles {
		writeOOMProfile(destPath, name)
	}

	writeOOMFile(destPath, "cmdline", []byte(strings.Join(os.Args, "\000")))
	writeOOMReportMetadata(destPath, memoryUsage, now)
	copyOOMConfigSnapshot(destPath)

	return nil
}

func writeOOMProfile(destPath string, name string) {
	profile := pprof.Lookup(name)
	if profile == nil {
		return
	}
	filePath := filepath.Join(destPath, name+".pb.gz")
	file, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer file.Close()
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()
	profile.WriteTo(gzipWriter, 0)
	if runtime.GOOS != "android" {
		os.Chown(filePath, sUserID, sGroupID)
	}
}

func writeOOMFile(destPath string, name string, content []byte) {
	filePath := filepath.Join(destPath, name)
	os.WriteFile(filePath, content, 0o666)
	if runtime.GOOS != "android" {
		os.Chown(filePath, sUserID, sGroupID)
	}
}

func writeOOMReportMetadata(destPath string, memoryUsage uint64, recordedAt time.Time) {
	processPath, _ := os.Executable()
	processName := filepath.Base(processPath)
	if processName == "." {
		processName = ""
	}
	metadata := oomReportMetadata{
		Source:      sCrashReportSource,
		ProcessName: processName,
		ProcessPath: processPath,
		RecordedAt:  recordedAt.Format(time.RFC3339),
		CoreVersion: C.Version,
		GoVersion:   GoVersion(),
		MemoryUsage: memoryUsage,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return
	}
	writeOOMFile(destPath, "metadata.json", data)
}

func copyOOMConfigSnapshot(destPath string) {
	snapshotPath := configSnapshotPath()
	content, err := os.ReadFile(snapshotPath)
	if err != nil || len(bytes.TrimSpace(content)) == 0 {
		return
	}
	writeOOMFile(destPath, "configuration.json", content)
}

func nextAvailableReportPath(reportsDir string, timestamp time.Time) string {
	destName := timestamp.Format("2006-01-02T15-04-05")
	destPath := filepath.Join(reportsDir, destName)
	for i := 1; ; i++ {
		_, err := os.Stat(destPath)
		if os.IsNotExist(err) {
			break
		}
		destPath = filepath.Join(reportsDir, destName+"-"+strconv.Itoa(i))
	}
	return destPath
}
