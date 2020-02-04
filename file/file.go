// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the utilities required for resolving file type and processing them
//There are sub directories which has implmentation for each file type supported by the system
package file

import (
	"path/filepath"
)

//Type denotes the type of the file
type Type int

//Following constants has the list of supported file types
const (
	//UNRESOLVED is the type that is not resolved or not supported
	UNRESOLVED Type = 0
	//CSV is the comma separated files ~ files ending with the extension .csv
	CSV Type = 1
)

//File interface has to be implemented by the file formats supported the platform
type File interface {
	//Clean will clean the existing file
	Clean() error
	//Upload will upload the data inside the file to the platform analytics engine
	Upload() error
}

//Separator is the file separator used by the underlying os
var Separator = string([]byte{filepath.Separator})

//ProcessFile will process a given file
func ProcessFile(filename string) {

}
