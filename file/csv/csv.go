// Copyright 2019 Cuttle.ai. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//Package csv has the implementation of the file interface for csv files
package csv

import (
	"github.com/cuttle-ai/file-uploader-service/config"
	"github.com/cuttle-ai/file-uploader-service/models"
)

//CSV handles the fiels of csv type
type CSV struct {
	//Filename is the name of the file
	Filename string
	//Name is the dataset name
	Name string
}

//Store stores the csv info to database
func (c *CSV) Store(a *config.AppContext) (*models.FileUpload, error) {
	result := &models.FileUpload{Name: c.Name, UserID: a.Session.User.ID, Location: c.Filename, Status: models.FileUploadStatusUploaded}
	err := a.Db.Create(result).Error
	return result, err
}

//Validate will validate the csv file and returns the errors existing while parsing the csv file
func (c *CSV) Validate() []error {
	return nil
}

//Clean will attemnpt to attempt to clean the file and reprt back the errors occurred while cleaning it
func (c *CSV) Clean() []error {
	return nil
}

//Upload will attempt to upload the file to the analytics engine and report any error occurred
func (c *CSV) Upload() error {
	return nil
}
