package transfer

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Manager handles file storage and retrieval for the chat application.
type Manager struct {
	baseDir   string
	pending   map[string]*pendingTransfer // fileID → partial file
	pendingMu sync.Mutex
}

type pendingTransfer struct {
	FileName   string
	TotalSize  int64
	Received   int64
	TotalChunks int
	Chunks     map[int][]byte
	FilePath   string
}

// NewManager creates a file manager rooted at baseDir.
func NewManager(baseDir string) *Manager {
	os.MkdirAll(baseDir, 0755)
	os.MkdirAll(filepath.Join(baseDir, "files"), 0755)
	os.MkdirAll(filepath.Join(baseDir, "images"), 0755)
	return &Manager{
		baseDir: baseDir,
		pending: make(map[string]*pendingTransfer),
	}
}

// BaseDir returns the root storage directory.
func (m *Manager) BaseDir() string {
	return m.baseDir
}

// =============================================================================
// Single file operations
// =============================================================================

// SaveFile writes received file data to disk.
// Returns the path where the file was saved.
func (m *Manager) SaveFile(fileName string, data []byte) (string, error) {
	subdir := "files"
	ext := strings.ToLower(filepath.Ext(fileName))
	if isImageExt(ext) {
		subdir = "images"
	}

	safeName := sanitizeFileName(fileName)
	destPath := filepath.Join(m.baseDir, subdir, safeName)

	// Avoid overwriting: append a number if file exists
	destPath = uniquePath(destPath)

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}
	log.Printf("[file] saved: %s (%d bytes)", destPath, len(data))
	return destPath, nil
}

// ReadFile reads a saved file. Only allows reads within baseDir.
func (m *Manager) ReadFile(relPath string) ([]byte, error) {
	fullPath := filepath.Join(m.baseDir, relPath)
	// Security: ensure path is within baseDir
	absBase, _ := filepath.Abs(m.baseDir)
	absPath, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absPath, absBase) {
		return nil, fmt.Errorf("path traversal denied")
	}
	return os.ReadFile(absPath)
}

// =============================================================================
// Chunked transfer (for large files)
// =============================================================================

// StartChunked begins a chunked file receive session.
func (m *Manager) StartChunked(fileID, fileName string, totalSize int64, totalChunks int) (string, error) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()

	safeName := sanitizeFileName(fileName)
	destPath := uniquePath(filepath.Join(m.baseDir, "files", safeName))

	m.pending[fileID] = &pendingTransfer{
		FileName:    fileName,
		TotalSize:   totalSize,
		TotalChunks: totalChunks,
		Chunks:      make(map[int][]byte),
		FilePath:    destPath,
	}

	log.Printf("[file] chunked receive started: %s (%d chunks, %d bytes)", fileName, totalChunks, totalSize)
	return fileID, nil
}

// ReceiveChunk stores a chunk of an ongoing file transfer.
func (m *Manager) ReceiveChunk(fileID string, chunkIndex int, data []byte) error {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()

	pt, ok := m.pending[fileID]
	if !ok {
		return fmt.Errorf("unknown transfer: %s", fileID)
	}

	pt.Chunks[chunkIndex] = data
	pt.Received += int64(len(data))
	return nil
}

// FinishChunked assembles all chunks and writes the complete file.
func (m *Manager) FinishChunked(fileID string) (string, error) {
	m.pendingMu.Lock()
	pt, ok := m.pending[fileID]
	if !ok {
		m.pendingMu.Unlock()
		return "", fmt.Errorf("unknown transfer: %s", fileID)
	}
	delete(m.pending, fileID)
	m.pendingMu.Unlock()

	// Assemble chunks in order
	if err := os.MkdirAll(filepath.Dir(pt.FilePath), 0755); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	f, err := os.Create(pt.FilePath)
	if err != nil {
		return "", fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	for i := 0; i < pt.TotalChunks; i++ {
		chunk, ok := pt.Chunks[i]
		if !ok {
			return "", fmt.Errorf("missing chunk %d for %s", i, fileID)
		}
		if _, err := f.Write(chunk); err != nil {
			return "", fmt.Errorf("write chunk %d: %w", i, err)
		}
	}

	log.Printf("[file] chunked receive completed: %s (%d bytes)", pt.FilePath, pt.TotalSize)
	return pt.FilePath, nil
}

// =============================================================================
// Folder operations
// =============================================================================

// PackFolder creates a .tar.gz archive of a folder and returns the path.
func (m *Manager) PackFolder(folderPath string) (string, error) {
	info, err := os.Stat(folderPath)
	if err != nil {
		return "", fmt.Errorf("stat folder: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %s", folderPath)
	}

	baseName := filepath.Base(folderPath)
	archiveName := baseName + ".tar.gz"
	archivePath := filepath.Join(m.baseDir, "files", archiveName)
	archivePath = uniquePath(archivePath)

	f, err := os.Create(archivePath)
	if err != nil {
		return "", fmt.Errorf("create archive: %w", err)
	}

	gw := gzip.NewWriter(f)
	tw := tar.NewWriter(gw)

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Create tar header
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		// Set the name relative to the folder
		relPath, _ := filepath.Rel(folderPath, path) // relative to folder itself, not parent
		header.Name = filepath.ToSlash(relPath)

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			if _, err := io.Copy(tw, file); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		tw.Close()
		gw.Close()
		f.Close()
		os.Remove(archivePath)
		return "", fmt.Errorf("pack folder: %w", err)
	}

	tw.Close()
	gw.Close()
	f.Close()

	log.Printf("[file] packed folder: %s → %s", folderPath, archivePath)
	return archivePath, nil
}

// UnpackFolder extracts a .tar.gz or .zip archive to the files directory.
func (m *Manager) UnpackFolder(archivePath string) (string, error) {
	ext := strings.ToLower(filepath.Ext(archivePath))
	baseName := strings.TrimSuffix(filepath.Base(archivePath), filepath.Ext(archivePath))
	destDir := filepath.Join(m.baseDir, "files", baseName)
	destDir = uniqueDir(destDir)

	switch ext {
	case ".gz":
		// It's actually .tar.gz — check for double extension
		if strings.HasSuffix(strings.ToLower(archivePath), ".tar.gz") {
			return destDir, m.extractTarGz(archivePath, destDir)
		}
		return "", fmt.Errorf("unsupported archive format: %s", ext)
	case ".zip":
		return destDir, m.extractZip(archivePath, destDir)
	default:
		return "", fmt.Errorf("unsupported archive format: %s", ext)
	}
}

func (m *Manager) extractTarGz(archivePath, destDir string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, filepath.FromSlash(header.Name))
		// Security: prevent path traversal
		absDest, _ := filepath.Abs(destDir)
		absTarget, _ := filepath.Abs(target)
		if !strings.HasPrefix(absTarget, absDest) {
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(target, 0755)
		case tar.TypeReg:
			os.MkdirAll(filepath.Dir(target), 0755)
			outFile, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
		}
	}
	return nil
}

func (m *Manager) extractZip(archivePath, destDir string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		target := filepath.Join(destDir, filepath.FromSlash(f.Name))
		absDest, _ := filepath.Abs(destDir)
		absTarget, _ := filepath.Abs(target)
		if !strings.HasPrefix(absTarget, absDest) {
			continue
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(target, 0755)
			continue
		}

		os.MkdirAll(filepath.Dir(target), 0755)
		rc, err := f.Open()
		if err != nil {
			return err
		}
		outFile, err := os.Create(target)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// Helpers
// =============================================================================

func sanitizeFileName(name string) string {
	// Remove path separators and other dangerous characters
	name = filepath.Base(name)
	name = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return '_'
		}
		return r
	}, name)
	if name == "" || name == "." {
		name = "unnamed"
	}
	return name
}

func uniquePath(path string) string {
	dir := filepath.Dir(path)
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(filepath.Base(path), ext)

	for i := 1; ; i++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return path
		}
		path = filepath.Join(dir, fmt.Sprintf("%s_%d%s", base, i, ext))
	}
}

func uniqueDir(path string) string {
	for i := 1; ; i++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return path
		}
		path = fmt.Sprintf("%s_%d", path, i)
	}
}

func isImageExt(ext string) bool {
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg", ".ico":
		return true
	}
	return false
}
