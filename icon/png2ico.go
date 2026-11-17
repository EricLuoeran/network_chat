//go:build ignore
// +build ignore

// png2ico converts a PNG to a multi-size ICO file.
// Usage: go run png2ico.go input.png output.ico

package main

import (
	"encoding/binary"
	"fmt"
	"image"
	"image/png"
	"os"

	"golang.org/x/image/draw"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: png2ico input.png output.ico\n")
		os.Exit(1)
	}
	if err := convert(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func convert(inPath, outPath string) error {
	f, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer f.Close()

	src, err := png.Decode(f)
	if err != nil {
		return fmt.Errorf("decode PNG: %w", err)
	}

	sizes := []int{16, 32, 48, 256}
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Collect PNG data for each size
	type entry struct {
		data   []byte
		width  int
		height int
	}
	var entries []entry
	offs := 6 + 16*len(sizes) // header + directory

	for _, sz := range sizes {
		// Resize
		resized := image.NewRGBA(image.Rect(0, 0, sz, sz))
		draw.BiLinear.Scale(resized, resized.Bounds(), src, src.Bounds(), draw.Over, nil)

		// Encode to PNG
		var buf []byte
		wp := &writeBuffer{}
		if err := png.Encode(wp, resized); err != nil {
			return fmt.Errorf("encode %dx%d: %w", sz, sz, err)
		}
		buf = wp.data

		entries = append(entries, entry{data: buf, width: sz, height: sz})
	}

	// Write ICO header
	binary.Write(out, binary.LittleEndian, uint16(0))  // reserved
	binary.Write(out, binary.LittleEndian, uint16(1))  // type = ICO
	binary.Write(out, binary.LittleEndian, uint16(len(entries)))

	// Write directory
	for _, e := range entries {
		w := byte(e.width)
		h := byte(e.height)
		if e.width >= 256 {
			w = 0
		}
		if e.height >= 256 {
			h = 0
		}
		out.Write([]byte{w, h, 0, 0})                        // w, h, colors, reserved
		binary.Write(out, binary.LittleEndian, uint16(1))     // planes
		binary.Write(out, binary.LittleEndian, uint16(32))    // bpp
		binary.Write(out, binary.LittleEndian, uint32(len(e.data))) // size
		binary.Write(out, binary.LittleEndian, uint32(offs))  // offset
		offs += len(e.data)
	}

	// Write image data
	for _, e := range entries {
		out.Write(e.data)
	}

	return nil
}

type writeBuffer struct {
	data []byte
}

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.data = append(w.data, p...)
	return len(p), nil
}
