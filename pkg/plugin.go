package flock

import (
	"fmt"
	"os"
	"os/exec"
	"plugin"
)

// PluginHandler ...
func PluginHandler(content []byte) (map[string]interface{}, error) {

	// Create plugin directory if it does not exist
	if _, err := os.Stat("./plugin"); os.IsNotExist(err) {
		// Modify permissions according to need
		if err := os.Mkdir("./plugin", 0777); err != nil {
			return nil, err
		}
	}

	// Write the byte stream to a file
	file, err := os.Create("./plugin/current.go")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	file.Write(content)

	// Compile the file as a plugin
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", "./plugin/current.so", "./plugin/current.go")
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	p, err := plugin.Open("plugin/current.so")
	if err != nil {
		return nil, err
	}

	fmSym, err := p.Lookup("FM")
	if err != nil {
		return nil, err
	}

	fmPlain, ok := fmSym.(*map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unable to assert interface as *map[string]interface{}, type: %T", fmSym)
	}

	fm := FuncMap(*fmPlain)

	return fm, nil
}
