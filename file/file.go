// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the utilities required for resolving file type and processing them
//There are sub directories which has implmentation for each file type supported by the system
package file

import (
	"errors"
	"strings"

	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/file/csv"
	"github.com/cuttle-ai/file-uploader-service/models"
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
	//Store stores the file info in the db so that it can be accessed later
	Store(*config.AppContext) (*models.FileUpload, error)
	//Validate will validate the file and returns the errors occurred
	Validate() []error
	//Clean will try to clean the existing file and returns the list of errors occurred
	Clean() []error
	//Upload will upload the data inside the file to the platform analytics engine
	Upload() error
}

//ProcessFile will process a given file
func ProcessFile(filename string, uploadname string) (File, error) {
	if strings.Index(filename, ".csv") == len(filename)-4 {
		return &csv.CSV{Filename: filename, Name: uploadname}, nil
	}
	return nil, errors.New("unidentified file format")
}
