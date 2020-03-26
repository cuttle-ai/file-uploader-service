// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package file has the utilities required for resolving file type and processing them
//There are sub directories which has implmentation for each file type supported by the system
package file

import (
	"errors"
	"fmt"
	"strings"

	brainModels "github.com/cuttle-ai/brain/models"
	"github.com/cuttle-ai/db-toolkit/datastores/services"
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/file/csv"
	"github.com/cuttle-ai/file-uploader-service/models"
	"github.com/cuttle-ai/file-uploader-service/models/db"
	"github.com/cuttle-ai/octopus/interpreter"
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
	Store(*config.AppContext) (*brainModels.Dataset, error)
	//Validate will validate the file and returns the errors occurred
	Validate() ([]error, error)
	//IdentifyColumns will try to identify the columns in the file. If no columns are passed as arguments, it will read from the file.
	//Else it will validate the given columns with the ones in the data file and try to refine the data type in the columns
	IdentifyColumns(columns []interpreter.ColumnNode) ([]interpreter.ColumnNode, error)
	//Upload will upload the data inside the file to the platform analytics engine replacing the existing data if the 3rd argument is true.
	Upload(*config.AppContext, interpreter.TableNode, bool, services.Service) error
	//UpdateStatus updates the status of the file in db
	UpdateStatus(*config.AppContext) error
	//ID returns the unique identified for the underlying resource in database
	ID() uint
}

//ProcessFile will process a given file
func ProcessFile(filename string, uploadname string) (File, error) {
	if strings.Index(filename, ".csv") == len(filename)-4 {
		return &csv.CSV{Filename: filename, Name: uploadname}, nil
	}
	return nil, errors.New("unidentified file format")
}

//GetFile will return the file interface if the type is valid
func GetFile(fileType string, fileModel db.FileUpload) (File, error) {
	if fileType == models.FileUploadTypeCSV {
		return &csv.CSV{Filename: fileModel.Location, Resource: fileModel}, nil
	}
	return nil, fmt.Errorf("unidentified file type %s", fileType)
}
