package store

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	dataFileName       = "data.db"
	logRecordFormat    = "SET %s %s\n"
	maxLogRecordLength = 1024 * 1024
)

type logFile struct {
	file *os.File
}

func openLog(dir string) (*logFile, error) {
	path := filepath.Join(dir, dataFileName)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log %s: %w", path, err)
	}

	return &logFile{file: file}, nil
}

func (l *logFile) append(key, value string) error {
	if _, err := fmt.Fprintf(l.file, logRecordFormat, key, value); err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync log: %w", err)
	}
	return nil
}

func replay(dir string, idx *index) error {
	path := filepath.Join(dir, dataFileName)

	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open log for replay: %w", err)
	}
	defer file.Close()

	return replayFrom(file, idx)
}

func replayFrom(reader io.Reader, idx *index) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 4096), maxLogRecordLength)

	for scanner.Scan() {
		parts := strings.SplitN(strings.TrimSpace(scanner.Text()), " ", 3)
		if len(parts) != 3 || !strings.EqualFold(parts[0], "SET") {
			continue
		}
		idx.set(parts[1], parts[2])
	}

	return scanner.Err()
}

func (l *logFile) close() error {
	return l.file.Close()
}
