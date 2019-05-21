// +build mage

package main

import "github.com/magefile/mage/sh"
import "github.com/magefile/mage/mg"

func RunServer() error {
	return sh.Run(mg.GoCmd(), "run", "./cmd/server/main.go")
}
