// +build mage

package main

import "github.com/magefile/mage/sh"

// BuildProto builds the protocol buffers for client server communication of flock
func BuildProto() error {
	return sh.Run("protoc",
		"-I", "/usr/local/include",
		"-I", ".",
		"--go_out=plugins=grpc:.",
		"./protos/flock.proto",
	)
}

// TestParser tests the parser
func TestParser() error {
	out, err := sh.Output("go", "test", "./pkg/")
	println(out)
	return err
}
