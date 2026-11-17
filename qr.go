package main

import (
	"bytes"
	"image/png"

	qrcode "github.com/skip2/go-qrcode"
)

func generateQR(url string) ([]byte, error) {
	img, err := qrcode.New(url, qrcode.Medium)
	if err != nil {
		return nil, err
	}
	img.DisableBorder = true
	var buf bytes.Buffer
	if err := png.Encode(&buf, img.Image(256)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
