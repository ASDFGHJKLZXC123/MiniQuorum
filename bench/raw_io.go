package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// NewStrictJSONDecoder returns a decoder that rejects unknown fields.
func NewStrictJSONDecoder(data []byte) *json.Decoder {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder
}

func decodeStrictJSON[T any](data []byte, out *T) error {
	decoder := NewStrictJSONDecoder(data)
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON content after value")
		}
		return err
	}
	return nil
}

// WriteRawResult writes a strict encoded raw result atomically.
func WriteRawResult(path string, result RawResult) error {
	if err := ValidateResult(result); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal raw result: %w", err)
	}
	encoded = append(encoded, '\n')
	return writeAtomicText(path, encoded)
}

// ReadRawResult decodes and validates a raw result file.
func ReadRawResult(path string) (RawResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RawResult{}, err
	}
	result, err := DecodeRawResult(data)
	if err != nil {
		return RawResult{}, err
	}
	return result, nil
}

// DecodeRawResult decodes bytes with strict unknown-field rejection and validation.
func DecodeRawResult(data []byte) (RawResult, error) {
	var result RawResult
	if err := decodeStrictJSON(data, &result); err != nil {
		return RawResult{}, err
	}
	if err := ValidateResult(result); err != nil {
		return RawResult{}, err
	}
	return result, nil
}

func writeAtomicText(path string, encoded []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".bench-output-*.tmp")
	if err != nil {
		return err
	}
	tmp := file.Name()
	_, writeErr := file.Write(encoded)
	closeErr := file.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
