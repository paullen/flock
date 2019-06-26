package flock

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
)

// PluginHandler ...
func PluginHandler(content []byte) (map[string]interface{}, error) {
	tmp := os.TempDir()

	gof := filepath.Join(tmp, "tmp.go")
	if err := ioutil.WriteFile(gof, content, 0666); err != nil {
		return nil, fmt.Errorf("failed to get file: %v", err)
	}

	sof := filepath.Join(tmp, "tmp.so")
	// Compile the file as a plugin
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", sof, gof)
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to compile: %v", err)
	}

	p, err := plugin.Open(sof)
	if err != nil {
		return nil, fmt.Errorf("failed to load plugin: %v", err)
	}

	fmSym, err := p.Lookup("FM")
	if err != nil {
		return nil, fmt.Errorf("failed to lookup FM: %v", err)
	}

	fmPlain, ok := fmSym.(*map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to assert interface as *map[string]interface{}, type: %T", fmSym)
	}

	fm := FuncMap(*fmPlain)

	return fm, nil
}
